//! Suffix-only hot engine for speed-first flashblock logs.

use std::sync::Arc;

use alloy_consensus::{
    Header, Sealed, TxReceipt,
    transaction::{Recovered, SignerRecoverable},
};
use alloy_eips::BlockNumberOrTag;
use alloy_primitives::{B256, BlockNumber};
use alloy_rpc_types::state::StateOverride;
use alloy_rpc_types_engine::PayloadId;
use base_common_chains::Upgrades;
use base_common_consensus::{BaseBlock, BaseTxEnvelope};
use base_common_evm::L1BlockInfo;
use base_common_flashblocks::{ExecutionPayloadBaseV1, Flashblock};
use base_execution_evm::BaseEvmConfig;
use reth_chainspec::{ChainSpecProvider, EthChainSpec};
use reth_evm::ConfigureEvm;
use reth_primitives_traits::RecoveredBlock;
use reth_provider::{BlockReaderIdExt, StateProviderFactory};
use reth_revm::{State, database::StateProviderDatabase};
use reth_storage_api::StateProviderBox;

use crate::{
    AuditWindowSnapshot, BlockAssembler, ExecutionError, FastFlashblockLog,
    FastFlashblockLogsDelta, FastFlashblockTxMeta, FlashblockSequenceValidator,
    FlashblockSnapshotId, HotDryRunSeed, HotExecutionState, HotPendingBlock, HotPendingWindow,
    HotSnapshot, HotWindowAnchor, Metrics, PendingStateBuilder, PeriodicAuditFailure,
    PeriodicAuditResult, ProviderError, Result, RetainedFastOutput, SequenceValidationResult,
    StateProcessorError,
    periodic_audit::{PeriodicAuditMismatchOrigin, PeriodicAuditOutputMismatch},
};

/// Concrete DB state carried by the hot engine across pending flashblocks.
pub type HotExecutionDb = State<StateProviderDatabase<StateProviderBox>>;

struct SuffixExecutionInput<'a> {
    flashblock: &'a Flashblock,
    wire_header_hash: B256,
    execution_block: BaseBlock,
    decoded_transactions: Vec<BaseTxEnvelope>,
    l1_block_info: L1BlockInfo,
    apply_pre_execution_changes: bool,
    pre_execution_parent_hash: B256,
    canonical_base_parent_hash: B256,
}

/// Result of applying one flashblock to the hot engine.
#[derive(Debug)]
pub enum HotApplyOutcome {
    /// A new delta and optional pinned snapshot were produced.
    Delta {
        /// Delta emitted for the applied flashblock.
        delta: Box<FastFlashblockLogsDelta>,
        /// Optional pinned snapshot derived from the same post-apply state.
        snapshot: Option<Box<HotSnapshot>>,
        /// Block number whose cached suffixes can now be drained, if any.
        ready_cached_block: Option<BlockNumber>,
    },
    /// The flashblock was already applied.
    Duplicate,
    /// Canonical reconciliation internally pruned the hot window without pubsub output.
    CanonicalWindowChanged,
    /// The hot engine requires a downstream reset/resync.
    Reset,
    /// The hot engine encountered an unrecoverable speculative inconsistency.
    InvalidateSession {
        /// Reason the current speculative session must be terminated.
        reason: HotInvalidationReason,
    },
}

/// Hard invalidation reason for a speculative hot session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HotInvalidationReason {
    /// A canonical block disagreed with previously retained reconciliation state.
    CanonicalConflict,
    /// Retained speculative suffixes could not be replayed soundly from canonical state.
    UnrecoverableReplayFailure,
    /// The speculative chain exceeded the anchor-relative depth limit and must fail closed.
    SpeculativeDepthExceeded,
    /// The live flashblock stream changed shape within an active speculative session.
    ContinuityViolation,
    /// Periodic shadow rebuild failed for the active retained prefix.
    PeriodicAuditFailed {
        /// Audit failure category reported by the shadow rebuild.
        failure: PeriodicAuditFailure,
    },
}

/// Outcome of completing one shadow rebuild against the live hot engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShadowRebuildCompletion {
    /// The rebuild remained equivalent and replaced the live retained window.
    EquivalentSwapped,
    /// The rebuild remained equivalent, but the live window advanced so no swap occurred.
    EquivalentNoOp,
    /// The result belonged to an older generation or window and was ignored.
    StaleIgnored,
    /// The active retained window diverged or failed and should be closed.
    ActiveFailed {
        /// Failure category reported by the active completion path.
        failure: PeriodicAuditFailure,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ShadowRebuildLiveRecheck {
    ReadyToSwap,
    EquivalentNoOp,
    ActiveFailed { failure: PeriodicAuditFailure },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReplayNonDeltaOutcome {
    Duplicate,
    CanonicalWindowChanged,
    Reset,
    InvalidateSession,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FlashblockPrefixMismatch {
    live_len: Option<usize>,
    mismatch_index: Option<usize>,
    expected_block_number: Option<BlockNumber>,
    live_block_number: Option<BlockNumber>,
    expected_flashblock_index: Option<u64>,
    live_flashblock_index: Option<u64>,
    expected_payload_id: Option<PayloadId>,
    live_payload_id: Option<PayloadId>,
    expected_parent_hash: Option<B256>,
    live_parent_hash: Option<B256>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RetainedOutputPrefixMismatch {
    live_len: Option<usize>,
    mismatch_index: Option<usize>,
    mismatch: Option<PeriodicAuditOutputMismatch>,
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
            flashblock.metadata.prev_flashblock_id,
        ) {
            SequenceValidationResult::NextInSequence => self.append_same_block_suffix(flashblock),
            SequenceValidationResult::FirstOfNextBlock => self.rollover_to_next_block(flashblock),
            SequenceValidationResult::Duplicate => {
                Ok(self.same_block_duplicate_outcome(flashblock))
            }
            SequenceValidationResult::NonSequentialGap { .. } => {
                Metrics::hot_window_reset_sequence_gap_count().increment(1);
                Ok(self.invalidate_session(HotInvalidationReason::ContinuityViolation))
            }
            SequenceValidationResult::InvalidNewBlockIndex { .. } => {
                Metrics::hot_window_reset_invalid_new_block_index_count().increment(1);
                Ok(self.invalidate_session(HotInvalidationReason::ContinuityViolation))
            }
            SequenceValidationResult::NonSequentialPredecessor { .. } => {
                Metrics::hot_window_reset_sequence_gap_count().increment(1);
                Ok(self.invalidate_session(HotInvalidationReason::ContinuityViolation))
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

        let anchor = self.window.anchor;
        let canonical_hash = block.header().hash_slow();

        if block.number < anchor.block_number() {
            return Ok(HotApplyOutcome::Duplicate);
        }

        if block.number == anchor.block_number() {
            return Ok(if canonical_hash == anchor.hash() {
                HotApplyOutcome::Duplicate
            } else {
                self.reset_for_canonical()
            });
        }

        let Some(oldest_pending_block) = self.window.blocks.front() else {
            return Ok(HotApplyOutcome::Duplicate);
        };
        let oldest_pending_block_number = oldest_pending_block.block_number;
        let latest_pending_block_number = self
            .window
            .blocks
            .back()
            .map(|pending_block| pending_block.block_number)
            .unwrap_or(oldest_pending_block_number);

        if block.number < oldest_pending_block_number || block.number > latest_pending_block_number
        {
            return Ok(self.reset_for_canonical());
        }

        if block.number != oldest_pending_block_number
            || !Self::canonical_block_matches_pending_block(oldest_pending_block, block)
        {
            return Ok(self.reset_for_canonical());
        }

        let Some(next_pending_block) = self.window.blocks.get(1) else {
            return Ok(self.reset_for_canonical());
        };

        if next_pending_block.parent_hash != canonical_hash {
            return Ok(self.reset_for_canonical());
        }

        self.rebase_retained_suffix_from_canonical_anchor(block, canonical_hash)
    }

    /// Rebuilds a fresh shadow hot engine from one immutable audit snapshot.
    pub fn rebuild_shadow_from_snapshot(
        client: Client,
        max_depth: u64,
        snapshot: &AuditWindowSnapshot,
    ) -> (PeriodicAuditResult, Option<Self>) {
        Self::rebuild_shadow_from_snapshot_with_cancellation(client, max_depth, snapshot, || false)
    }

    /// Rebuilds a shadow hot engine while cooperatively honoring cancellation checks.
    pub fn rebuild_shadow_from_snapshot_with_cancellation<F>(
        client: Client,
        max_depth: u64,
        snapshot: &AuditWindowSnapshot,
        mut is_cancelled: F,
    ) -> (PeriodicAuditResult, Option<Self>)
    where
        F: FnMut() -> bool,
    {
        match Self::canonical_anchor_matches_snapshot(&client, snapshot) {
            Ok(true) => {}
            Ok(false) => {
                return (
                    PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::AnchorMismatch },
                    None,
                );
            }
            Err(_error) => {
                return (
                    PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::WorkerError },
                    None,
                );
            }
        }

        if !snapshot.first_replayed_flashblock_matches_anchor() {
            Self::log_snapshot_anchor_shape_mismatch(snapshot);
            return (
                PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::Mismatch },
                None,
            );
        }

        let mut shadow_engine = Self::new(client, max_depth);
        let mut rebuilt_outputs = Vec::with_capacity(snapshot.flashblocks.len());

        for flashblock in &snapshot.flashblocks {
            if is_cancelled() {
                return (PeriodicAuditResult::StaleIgnored, None);
            }

            let outcome = match shadow_engine.apply_flashblock(flashblock) {
                Ok(outcome) => outcome,
                Err(_error) => {
                    return (
                        PeriodicAuditResult::Diverged {
                            failure: PeriodicAuditFailure::WorkerError,
                        },
                        None,
                    );
                }
            };

            match outcome {
                HotApplyOutcome::Delta { delta, .. } => {
                    rebuilt_outputs.push(RetainedFastOutput::from_delta(&delta));
                }
                HotApplyOutcome::Duplicate => {
                    Self::log_replay_non_delta_outcome(
                        snapshot,
                        flashblock,
                        ReplayNonDeltaOutcome::Duplicate,
                        None,
                    );
                    return (
                        PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::Mismatch },
                        None,
                    );
                }
                HotApplyOutcome::CanonicalWindowChanged => {
                    Self::log_replay_non_delta_outcome(
                        snapshot,
                        flashblock,
                        ReplayNonDeltaOutcome::CanonicalWindowChanged,
                        None,
                    );
                    return (
                        PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::Mismatch },
                        None,
                    );
                }
                HotApplyOutcome::Reset => {
                    Self::log_replay_non_delta_outcome(
                        snapshot,
                        flashblock,
                        ReplayNonDeltaOutcome::Reset,
                        None,
                    );
                    return (
                        PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::Mismatch },
                        None,
                    );
                }
                HotApplyOutcome::InvalidateSession { reason } => {
                    Self::log_replay_non_delta_outcome(
                        snapshot,
                        flashblock,
                        ReplayNonDeltaOutcome::InvalidateSession,
                        Some(reason),
                    );
                    return (
                        PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::Mismatch },
                        None,
                    );
                }
            }
        }

        if is_cancelled() {
            return (PeriodicAuditResult::StaleIgnored, None);
        }

        let audit_result = snapshot.compare_rebuilt_outputs(&rebuilt_outputs);

        if audit_result == PeriodicAuditResult::EquivalentPrefix {
            (audit_result, Some(shadow_engine))
        } else {
            (audit_result, None)
        }
    }

    fn replay_shadow_from_snapshot_for_canonical_rebase(
        client: Client,
        max_depth: u64,
        snapshot: &AuditWindowSnapshot,
    ) -> (PeriodicAuditResult, Option<Self>) {
        match Self::canonical_anchor_matches_snapshot(&client, snapshot) {
            Ok(true) => {}
            Ok(false) => {
                return (
                    PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::AnchorMismatch },
                    None,
                );
            }
            Err(_error) => {
                return (
                    PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::WorkerError },
                    None,
                );
            }
        }

        if !snapshot.first_replayed_flashblock_matches_anchor() {
            Self::log_snapshot_anchor_shape_mismatch(snapshot);
            return (
                PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::Mismatch },
                None,
            );
        }

        let mut shadow_engine = Self::new(client, max_depth);

        for flashblock in &snapshot.flashblocks {
            let outcome = match shadow_engine.apply_flashblock(flashblock) {
                Ok(outcome) => outcome,
                Err(_error) => {
                    return (
                        PeriodicAuditResult::Diverged {
                            failure: PeriodicAuditFailure::WorkerError,
                        },
                        None,
                    );
                }
            };

            match outcome {
                HotApplyOutcome::Delta { .. } => {}
                HotApplyOutcome::Duplicate => {
                    Self::log_replay_non_delta_outcome(
                        snapshot,
                        flashblock,
                        ReplayNonDeltaOutcome::Duplicate,
                        None,
                    );
                    return (
                        PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::Mismatch },
                        None,
                    );
                }
                HotApplyOutcome::CanonicalWindowChanged => {
                    Self::log_replay_non_delta_outcome(
                        snapshot,
                        flashblock,
                        ReplayNonDeltaOutcome::CanonicalWindowChanged,
                        None,
                    );
                    return (
                        PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::Mismatch },
                        None,
                    );
                }
                HotApplyOutcome::Reset => {
                    Self::log_replay_non_delta_outcome(
                        snapshot,
                        flashblock,
                        ReplayNonDeltaOutcome::Reset,
                        None,
                    );
                    return (
                        PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::Mismatch },
                        None,
                    );
                }
                HotApplyOutcome::InvalidateSession { reason } => {
                    Self::log_replay_non_delta_outcome(
                        snapshot,
                        flashblock,
                        ReplayNonDeltaOutcome::InvalidateSession,
                        Some(reason),
                    );
                    return (
                        PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::Mismatch },
                        None,
                    );
                }
            }
        }

        (PeriodicAuditResult::EquivalentPrefix, Some(shadow_engine))
    }

    /// Completes one shadow rebuild against the current live hot engine.
    pub fn complete_shadow_rebuild(
        &mut self,
        active_generation: u64,
        active_window_id: u64,
        snapshot: &AuditWindowSnapshot,
        audit_result: PeriodicAuditResult,
        rebuilt_engine: Option<Self>,
    ) -> ShadowRebuildCompletion {
        if snapshot.generation != active_generation || snapshot.window_id != active_window_id {
            return ShadowRebuildCompletion::StaleIgnored;
        }

        match audit_result {
            PeriodicAuditResult::EquivalentPrefix => match self
                .recheck_live_window_against_snapshot(snapshot)
            {
                ShadowRebuildLiveRecheck::ReadyToSwap => {
                    let Some(mut rebuilt_engine) = rebuilt_engine else {
                        self.reset();
                        return ShadowRebuildCompletion::ActiveFailed {
                            failure: PeriodicAuditFailure::WorkerError,
                        };
                    };

                    let next_snapshot_nonce = self.next_snapshot_nonce;
                    rebuilt_engine.next_snapshot_nonce = next_snapshot_nonce;
                    self.window = rebuilt_engine.window;
                    self.next_snapshot_nonce = next_snapshot_nonce;

                    ShadowRebuildCompletion::EquivalentSwapped
                }
                ShadowRebuildLiveRecheck::EquivalentNoOp => ShadowRebuildCompletion::EquivalentNoOp,
                ShadowRebuildLiveRecheck::ActiveFailed { failure } => {
                    self.reset();
                    ShadowRebuildCompletion::ActiveFailed { failure }
                }
            },
            PeriodicAuditResult::Diverged { failure } => {
                self.reset();
                ShadowRebuildCompletion::ActiveFailed { failure }
            }
            PeriodicAuditResult::StaleIgnored => ShadowRebuildCompletion::StaleIgnored,
        }
    }

    fn recheck_live_window_against_snapshot(
        &self,
        snapshot: &AuditWindowSnapshot,
    ) -> ShadowRebuildLiveRecheck {
        if self.window.anchor
            != HotWindowAnchor::new(snapshot.anchor_block_number, snapshot.anchor_hash)
        {
            self.log_live_anchor_mismatch(snapshot);
            return ShadowRebuildLiveRecheck::ActiveFailed {
                failure: PeriodicAuditFailure::AnchorMismatch,
            };
        }

        let live_flashblocks =
            self.window.retained_flashblocks_from_anchor(snapshot.anchor_block_number);
        if let Some(mismatch) =
            Self::flashblock_prefix_mismatch(&snapshot.flashblocks, live_flashblocks.as_deref())
        {
            Self::log_live_flashblock_prefix_mismatch(snapshot, &mismatch);
            return ShadowRebuildLiveRecheck::ActiveFailed {
                failure: PeriodicAuditFailure::Mismatch,
            };
        }

        let live_outputs = self.window.retained_outputs_from_anchor(snapshot.anchor_block_number);
        if let Some(mismatch) = Self::retained_output_prefix_mismatch(
            &snapshot.expected_outputs,
            live_outputs.as_deref(),
        ) {
            Self::log_live_output_prefix_mismatch(snapshot, mismatch);
            return ShadowRebuildLiveRecheck::ActiveFailed {
                failure: PeriodicAuditFailure::Mismatch,
            };
        }

        let live_flashblocks = live_flashblocks
            .expect("matching live flashblocks should be available after prefix check");
        let live_outputs =
            live_outputs.expect("matching live outputs should be available after prefix check");

        if live_flashblocks.len() == snapshot.flashblocks.len()
            && live_outputs.len() == snapshot.expected_outputs.len()
            && self.window.latest_audit_cursor() == Some(snapshot.cursor)
        {
            ShadowRebuildLiveRecheck::ReadyToSwap
        } else {
            ShadowRebuildLiveRecheck::EquivalentNoOp
        }
    }

    /// Resets the current pending hot window.
    pub fn reset(&mut self) {
        self.window.reset();
    }

    /// Forks a request-local dry-run seed from the live hot execution state when it still matches
    /// the supplied hot snapshot.
    pub fn fork_dry_run_seed(&self, hot_snapshot: Arc<HotSnapshot>) -> Option<HotDryRunSeed> {
        let active_block = self.window.active_block()?;
        let execution = self.window.execution.as_ref()?;

        if active_block.block_number != hot_snapshot.snapshot_id.block_number()
            || active_block.latest_flashblock_index != hot_snapshot.snapshot_id.flashblock_index()
            || active_block.payload_id != hot_snapshot.snapshot_id.payload_id()
            || active_block.parent_hash != hot_snapshot.snapshot_id.parent_hash()
            || active_block.latest_header.hash() != hot_snapshot.latest_header.hash()
            || execution.last_header.hash() != hot_snapshot.latest_header.hash()
        {
            return None;
        }

        Some(HotDryRunSeed::from_execution(hot_snapshot, execution))
    }

    fn invalidate_session(&mut self, reason: HotInvalidationReason) -> HotApplyOutcome {
        self.reset();
        HotApplyOutcome::InvalidateSession { reason }
    }

    const fn next_snapshot_nonce(&mut self) -> u64 {
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

    fn rebase_retained_suffix_from_canonical_anchor(
        &mut self,
        canonical_block: &RecoveredBlock<BaseBlock>,
        canonical_hash: B256,
    ) -> Result<HotApplyOutcome> {
        let Some(snapshot) = self.retained_suffix_snapshot_after_canonical_anchor(
            canonical_block.number,
            canonical_hash,
        ) else {
            warn!(
                message = "canonical suffix rebase snapshot missing",
                canonical_block_number = canonical_block.number,
                canonical_hash = %canonical_hash,
            );
            return Ok(self.invalidate_session(HotInvalidationReason::UnrecoverableReplayFailure));
        };

        let (audit_result, rebuilt_engine) = Self::replay_shadow_from_snapshot_for_canonical_rebase(
            self.client.clone(),
            self.max_depth,
            &snapshot,
        );

        match audit_result {
            PeriodicAuditResult::EquivalentPrefix => {
                let Some(rebuilt_engine) = rebuilt_engine else {
                    warn!(
                        message = "canonical suffix rebase missing rebuilt engine",
                        canonical_block_number = canonical_block.number,
                        canonical_hash = %canonical_hash,
                    );
                    return Ok(
                        self.invalidate_session(HotInvalidationReason::UnrecoverableReplayFailure)
                    );
                };

                self.window = rebuilt_engine.window;
                Ok(HotApplyOutcome::CanonicalWindowChanged)
            }
            PeriodicAuditResult::Diverged { failure } => {
                warn!(
                    message = "canonical suffix rebase diverged",
                    canonical_block_number = canonical_block.number,
                    canonical_hash = %canonical_hash,
                    failure = ?failure,
                );
                Ok(self.invalidate_session(HotInvalidationReason::UnrecoverableReplayFailure))
            }
            PeriodicAuditResult::StaleIgnored => {
                warn!(
                    message = "canonical suffix rebase returned stale result",
                    canonical_block_number = canonical_block.number,
                    canonical_hash = %canonical_hash,
                );
                Ok(self.invalidate_session(HotInvalidationReason::UnrecoverableReplayFailure))
            }
        }
    }

    fn retained_suffix_snapshot_after_canonical_anchor(
        &self,
        canonical_block_number: BlockNumber,
        canonical_hash: B256,
    ) -> Option<AuditWindowSnapshot> {
        let flashblocks = self
            .window
            .blocks
            .iter()
            .skip(1)
            .flat_map(|block| block.flashblocks.iter().cloned())
            .collect::<Vec<_>>();
        let expected_outputs = self
            .window
            .blocks
            .iter()
            .skip(1)
            .flat_map(|block| block.published_outputs.iter().cloned())
            .collect::<Vec<_>>();
        let cursor = expected_outputs.last().map(|output| output.cursor)?;

        Some(AuditWindowSnapshot::new(
            0,
            0,
            canonical_block_number,
            canonical_hash,
            cursor,
            flashblocks,
            expected_outputs,
        ))
    }

    fn canonical_anchor_matches_snapshot(
        client: &Client,
        snapshot: &AuditWindowSnapshot,
    ) -> Result<bool> {
        let Some(anchor_header) = client
            .header_by_number(snapshot.anchor_block_number)
            .map_err(|error| ProviderError::StateProvider(error.to_string()))?
        else {
            return Ok(false);
        };

        Ok(anchor_header.hash_slow() == snapshot.anchor_hash)
    }

    fn same_block_continuity_outcome(
        &mut self,
        flashblock: &Flashblock,
    ) -> Option<HotApplyOutcome> {
        let Some(active_block) = self.window.active_block() else {
            Metrics::hot_window_reset_missing_active_block_count().increment(1);
            return Some(self.reset_for_flashblock());
        };

        let payload_id_mismatch = flashblock.payload_id != active_block.payload_id;
        let base_mismatch = flashblock.base.as_ref().is_some_and(|base| base != &active_block.base);

        if payload_id_mismatch {
            return Some(self.invalidate_session(HotInvalidationReason::ContinuityViolation));
        }

        if base_mismatch {
            Metrics::hot_window_reset_same_block_parent_mismatch_count().increment(1);
            return Some(self.invalidate_session(HotInvalidationReason::ContinuityViolation));
        }

        None
    }

    fn same_block_duplicate_outcome(&mut self, flashblock: &Flashblock) -> HotApplyOutcome {
        if let Some(outcome) = self.same_block_continuity_outcome(flashblock) {
            return outcome;
        }

        let Some(active_block) = self.window.active_block() else {
            Metrics::hot_window_reset_missing_active_block_count().increment(1);
            return self.reset_for_flashblock();
        };

        match usize::try_from(flashblock.index)
            .ok()
            .and_then(|index| active_block.flashblocks.get(index))
        {
            Some(previous_flashblock) if previous_flashblock == flashblock => {
                HotApplyOutcome::Duplicate
            }
            _ => self.invalidate_session(HotInvalidationReason::ContinuityViolation),
        }
    }

    fn log_snapshot_anchor_shape_mismatch(snapshot: &AuditWindowSnapshot) {
        let first_flashblock = snapshot.flashblocks.first();

        warn!(
            message = "periodic audit snapshot anchor mismatch",
            audit_origin = ?PeriodicAuditMismatchOrigin::SnapshotAnchorShape,
            generation = snapshot.generation,
            window_id = snapshot.window_id,
            anchor_block_number = snapshot.anchor_block_number,
            anchor_hash = %snapshot.anchor_hash,
            audit_cursor = ?snapshot.cursor,
            expected_first_block_number = ?snapshot.anchor_block_number.checked_add(1),
            actual_first_block_number = ?first_flashblock.map(|flashblock| flashblock.metadata.block_number),
            actual_first_flashblock_index = ?first_flashblock.map(|flashblock| flashblock.index),
            actual_first_payload_id = ?first_flashblock.map(|flashblock| flashblock.payload_id),
            actual_first_parent_hash = ?first_flashblock.and_then(Self::flashblock_parent_hash),
            retained_flashblocks = snapshot.flashblocks.len(),
            retained_outputs = snapshot.expected_outputs.len(),
        );
    }

    fn log_replay_non_delta_outcome(
        snapshot: &AuditWindowSnapshot,
        flashblock: &Flashblock,
        replay_outcome: ReplayNonDeltaOutcome,
        replay_invalidation_reason: Option<HotInvalidationReason>,
    ) {
        warn!(
            message = "periodic audit replay returned non-delta outcome",
            audit_origin = ?PeriodicAuditMismatchOrigin::ReplayNonDeltaOutcome,
            generation = snapshot.generation,
            window_id = snapshot.window_id,
            anchor_block_number = snapshot.anchor_block_number,
            anchor_hash = %snapshot.anchor_hash,
            audit_cursor = ?snapshot.cursor,
            replay_block_number = flashblock.metadata.block_number,
            replay_flashblock_index = flashblock.index,
            replay_payload_id = ?flashblock.payload_id,
            replay_parent_hash = ?Self::flashblock_parent_hash(flashblock),
            replay_outcome = ?replay_outcome,
            replay_invalidation_reason = ?replay_invalidation_reason,
        );
    }

    fn log_live_anchor_mismatch(&self, snapshot: &AuditWindowSnapshot) {
        warn!(
            message = "periodic audit live recheck mismatch",
            audit_origin = ?PeriodicAuditMismatchOrigin::LiveAnchor,
            generation = snapshot.generation,
            window_id = snapshot.window_id,
            anchor_block_number = snapshot.anchor_block_number,
            anchor_hash = %snapshot.anchor_hash,
            audit_cursor = ?snapshot.cursor,
            live_anchor_block_number = self.window.anchor.block_number(),
            live_anchor_hash = %self.window.anchor.hash(),
        );
    }

    fn log_live_flashblock_prefix_mismatch(
        snapshot: &AuditWindowSnapshot,
        mismatch: &FlashblockPrefixMismatch,
    ) {
        warn!(
            message = "periodic audit live recheck mismatch",
            audit_origin = ?PeriodicAuditMismatchOrigin::LiveFlashblockPrefix,
            generation = snapshot.generation,
            window_id = snapshot.window_id,
            anchor_block_number = snapshot.anchor_block_number,
            anchor_hash = %snapshot.anchor_hash,
            audit_cursor = ?snapshot.cursor,
            expected_len = snapshot.flashblocks.len(),
            live_len = ?mismatch.live_len,
            mismatch_index = ?mismatch.mismatch_index,
            expected_block_number = ?mismatch.expected_block_number,
            live_block_number = ?mismatch.live_block_number,
            expected_flashblock_index = ?mismatch.expected_flashblock_index,
            live_flashblock_index = ?mismatch.live_flashblock_index,
            expected_payload_id = ?mismatch.expected_payload_id,
            live_payload_id = ?mismatch.live_payload_id,
            expected_parent_hash = ?mismatch.expected_parent_hash,
            live_parent_hash = ?mismatch.live_parent_hash,
        );
    }

    fn log_live_output_prefix_mismatch(
        snapshot: &AuditWindowSnapshot,
        mismatch: RetainedOutputPrefixMismatch,
    ) {
        warn!(
            message = "periodic audit live recheck mismatch",
            audit_origin = ?PeriodicAuditMismatchOrigin::LiveOutputPrefix,
            generation = snapshot.generation,
            window_id = snapshot.window_id,
            anchor_block_number = snapshot.anchor_block_number,
            anchor_hash = %snapshot.anchor_hash,
            audit_cursor = ?snapshot.cursor,
            expected_len = snapshot.expected_outputs.len(),
            live_len = ?mismatch.live_len,
            mismatch_index = ?mismatch.mismatch_index,
            mismatch_field = ?mismatch.mismatch.map(|mismatch| mismatch.field),
            expected_cursor = ?mismatch.mismatch.map(|mismatch| mismatch.expected_cursor),
            live_cursor = ?mismatch.mismatch.map(|mismatch| mismatch.actual_cursor),
        );
    }

    fn flashblock_parent_hash(flashblock: &Flashblock) -> Option<B256> {
        flashblock.base.as_ref().map(|base| base.parent_hash)
    }

    fn flashblock_wire_header_hash(flashblock: &Flashblock) -> Option<B256> {
        (flashblock.diff.block_hash != B256::ZERO).then_some(flashblock.diff.block_hash)
    }

    fn cumulative_wire_header_hash(flashblocks: &[Flashblock]) -> Result<B256> {
        Ok(BlockAssembler::assemble(flashblocks)?.block.header.hash_slow())
    }

    fn flashblock_prefix_mismatch(
        expected_prefix: &[Flashblock],
        live_flashblocks: Option<&[Flashblock]>,
    ) -> Option<FlashblockPrefixMismatch> {
        let Some(live_flashblocks) = live_flashblocks else {
            return (!expected_prefix.is_empty()).then(|| FlashblockPrefixMismatch {
                live_len: None,
                mismatch_index: Some(0),
                expected_block_number: expected_prefix
                    .first()
                    .map(|flashblock| flashblock.metadata.block_number),
                live_block_number: None,
                expected_flashblock_index: expected_prefix
                    .first()
                    .map(|flashblock| flashblock.index),
                live_flashblock_index: None,
                expected_payload_id: expected_prefix
                    .first()
                    .map(|flashblock| flashblock.payload_id),
                live_payload_id: None,
                expected_parent_hash: expected_prefix
                    .first()
                    .and_then(Self::flashblock_parent_hash),
                live_parent_hash: None,
            });
        };

        if let Some(mismatch_index) = expected_prefix.iter().zip(live_flashblocks).position(
            |(expected_flashblock, live_flashblock)| expected_flashblock != live_flashblock,
        ) {
            return Some(Self::flashblock_prefix_mismatch_at_index(
                expected_prefix,
                live_flashblocks,
                mismatch_index,
            ));
        }

        (live_flashblocks.len() < expected_prefix.len()).then(|| {
            Self::flashblock_prefix_mismatch_at_index(
                expected_prefix,
                live_flashblocks,
                live_flashblocks.len(),
            )
        })
    }

    fn flashblock_prefix_mismatch_at_index(
        expected_prefix: &[Flashblock],
        live_flashblocks: &[Flashblock],
        mismatch_index: usize,
    ) -> FlashblockPrefixMismatch {
        let expected_flashblock = expected_prefix.get(mismatch_index);
        let live_flashblock = live_flashblocks.get(mismatch_index);

        FlashblockPrefixMismatch {
            live_len: Some(live_flashblocks.len()),
            mismatch_index: Some(mismatch_index),
            expected_block_number: expected_flashblock
                .map(|flashblock| flashblock.metadata.block_number),
            live_block_number: live_flashblock.map(|flashblock| flashblock.metadata.block_number),
            expected_flashblock_index: expected_flashblock.map(|flashblock| flashblock.index),
            live_flashblock_index: live_flashblock.map(|flashblock| flashblock.index),
            expected_payload_id: expected_flashblock.map(|flashblock| flashblock.payload_id),
            live_payload_id: live_flashblock.map(|flashblock| flashblock.payload_id),
            expected_parent_hash: expected_flashblock.and_then(Self::flashblock_parent_hash),
            live_parent_hash: live_flashblock.and_then(Self::flashblock_parent_hash),
        }
    }

    fn retained_output_prefix_mismatch(
        expected_prefix: &[RetainedFastOutput],
        live_outputs: Option<&[RetainedFastOutput]>,
    ) -> Option<RetainedOutputPrefixMismatch> {
        let Some(live_outputs) = live_outputs else {
            return (!expected_prefix.is_empty()).then_some(RetainedOutputPrefixMismatch {
                live_len: None,
                mismatch_index: Some(0),
                mismatch: None,
            });
        };

        if let Some(mismatch) = RetainedFastOutput::first_mismatch(expected_prefix, live_outputs) {
            return Some(RetainedOutputPrefixMismatch {
                live_len: Some(live_outputs.len()),
                mismatch_index: Some(mismatch.index),
                mismatch: Some(mismatch),
            });
        }

        (live_outputs.len() < expected_prefix.len()).then_some(RetainedOutputPrefixMismatch {
            live_len: Some(live_outputs.len()),
            mismatch_index: Some(live_outputs.len()),
            mismatch: None,
        })
    }

    const fn speculative_depth_after_next_block(
        &self,
        next_block_number: BlockNumber,
    ) -> BlockNumber {
        next_block_number.saturating_sub(self.window.anchor.block_number())
    }

    fn start_first_flashblock(&mut self, flashblock: &Flashblock) -> Result<HotApplyOutcome> {
        let Some(wire_header_hash) = Self::flashblock_wire_header_hash(flashblock) else {
            return Ok(self.reset_for_flashblock());
        };
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
            last_header: Self::hash_existing_header(canonical_header),
            state_overrides: StateOverride::default(),
            l1_block_info: L1BlockInfo::default(),
        };

        let (execution, pending_block, delta, snapshot) = self.execute_new_block_suffix(
            flashblock,
            execution,
            base,
            wire_header_hash,
            canonical_parent_hash,
            base_parent_hash,
        )?;

        self.window.anchor = HotWindowAnchor::new(canonical_block_number, base_parent_hash);
        self.window.execution = Some(execution);
        self.window.push_block(pending_block);

        Ok(HotApplyOutcome::Delta {
            delta: Box::new(delta),
            snapshot: Some(Box::new(snapshot)),
            ready_cached_block: Some(flashblock.metadata.block_number),
        })
    }

    fn append_same_block_suffix(&mut self, flashblock: &Flashblock) -> Result<HotApplyOutcome> {
        if let Some(outcome) = self.same_block_continuity_outcome(flashblock) {
            return Ok(outcome);
        }

        let Some(wire_header_hash) = Self::flashblock_wire_header_hash(flashblock) else {
            return Ok(self.invalidate_session(HotInvalidationReason::ContinuityViolation));
        };

        let canonical_base_parent_hash = self.window.anchor.hash();
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
            execution,
            pending_block,
            SuffixExecutionInput {
                flashblock,
                wire_header_hash,
                execution_block,
                decoded_transactions,
                l1_block_info: carried_l1_block_info,
                apply_pre_execution_changes: false,
                pre_execution_parent_hash: B256::ZERO,
                canonical_base_parent_hash,
            },
        )?;

        self.window.execution = Some(execution);
        self.window.blocks.push_back(pending_block);

        Ok(HotApplyOutcome::Delta {
            delta: Box::new(delta),
            snapshot: Some(Box::new(snapshot)),
            ready_cached_block: None,
        })
    }

    fn rollover_to_next_block(&mut self, flashblock: &Flashblock) -> Result<HotApplyOutcome> {
        let _rollover_timer = base_metrics::timed!(Metrics::hot_window_rollover_duration());

        let base = match BlockAssembler::base_from_first_flashblock(flashblock) {
            Ok(base) => base,
            Err(StateProcessorError::Protocol(_)) => {
                return Ok(self.invalidate_session(HotInvalidationReason::ContinuityViolation));
            }
            Err(error) => return Err(error),
        };
        let pre_execution_parent_hash = base.parent_hash;

        if self.speculative_depth_after_next_block(flashblock.metadata.block_number)
            > self.max_depth
        {
            return Ok(self.invalidate_session(HotInvalidationReason::SpeculativeDepthExceeded));
        }

        let Some(wire_header_hash) = Self::flashblock_wire_header_hash(flashblock) else {
            return Ok(self.invalidate_session(HotInvalidationReason::ContinuityViolation));
        };

        let canonical_base_parent_hash = self.window.anchor.hash();
        let execution = self.window.execution.take().ok_or_else(|| {
            StateProcessorError::HotEngine("missing hot execution state".to_string())
        })?;

        let (execution_block, decoded_transactions) =
            Self::prepare_suffix_block(&base, flashblock)?;
        let l1_block_info = Self::extract_l1_block_info(&execution_block)?;
        let pending_block = HotPendingBlock {
            block_number: flashblock.metadata.block_number,
            payload_id: flashblock.payload_id,
            base: base.clone(),
            parent_hash: base.parent_hash,
            latest_flashblock_index: 0,
            latest_wire_header_hash: wire_header_hash,
            latest_header: Self::hash_existing_header(execution_block.header.clone()),
            next_tx_index: 0,
            next_log_index: 0,
            cumulative_gas_used: 0,
            transactions: vec![],
            logs: vec![],
            receipts: std::collections::HashMap::new(),
            rpc_transactions: std::collections::HashMap::new(),
            transaction_senders: std::collections::HashMap::new(),
            flashblocks: vec![],
            published_outputs: vec![],
            local_header_parts: None,
        };

        let (execution, pending_block, delta, snapshot) = self.execute_suffix(
            execution,
            pending_block,
            SuffixExecutionInput {
                flashblock,
                wire_header_hash,
                execution_block,
                decoded_transactions,
                l1_block_info,
                apply_pre_execution_changes: true,
                pre_execution_parent_hash,
                canonical_base_parent_hash,
            },
        )?;

        self.window.execution = Some(execution);
        self.window.push_block(pending_block);

        Ok(HotApplyOutcome::Delta {
            delta: Box::new(delta),
            snapshot: Some(Box::new(snapshot)),
            ready_cached_block: Some(flashblock.metadata.block_number),
        })
    }

    fn execute_new_block_suffix(
        &mut self,
        flashblock: &Flashblock,
        execution: HotExecutionState<HotExecutionDb>,
        base: ExecutionPayloadBaseV1,
        wire_header_hash: B256,
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
            latest_wire_header_hash: wire_header_hash,
            latest_header: Self::hash_existing_header(execution_block.header.clone()),
            next_tx_index: 0,
            next_log_index: 0,
            cumulative_gas_used: 0,
            transactions: vec![],
            logs: vec![],
            receipts: std::collections::HashMap::new(),
            rpc_transactions: std::collections::HashMap::new(),
            transaction_senders: std::collections::HashMap::new(),
            flashblocks: vec![],
            published_outputs: vec![],
            local_header_parts: None,
        };

        self.execute_suffix(
            execution,
            pending_block,
            SuffixExecutionInput {
                flashblock,
                wire_header_hash,
                execution_block,
                decoded_transactions,
                l1_block_info,
                apply_pre_execution_changes: true,
                pre_execution_parent_hash,
                canonical_base_parent_hash,
            },
        )
    }

    fn execute_suffix(
        &mut self,
        mut execution: HotExecutionState<HotExecutionDb>,
        mut pending_block: HotPendingBlock,
        input: SuffixExecutionInput<'_>,
    ) -> Result<(
        HotExecutionState<HotExecutionDb>,
        HotPendingBlock,
        FastFlashblockLogsDelta,
        HotSnapshot,
    )> {
        let SuffixExecutionInput {
            flashblock,
            wire_header_hash: _wire_header_hash,
            execution_block,
            decoded_transactions,
            l1_block_info,
            apply_pre_execution_changes,
            pre_execution_parent_hash,
            canonical_base_parent_hash,
        } = input;
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
            (cumulative_gas_used, next_log_index_usize),
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
        pending_block.published_outputs.push(RetainedFastOutput::from_delta(&delta));
        pending_block.local_header_parts = None;
        let latest_wire_header_hash =
            Self::cumulative_wire_header_hash(&pending_block.flashblocks)?;
        let (db, state_overrides) = pending_state_builder.into_db_and_state_overrides();
        // Exact local sealing is only needed at rollover/canonical boundaries. Keep the latest
        // suffix header for snapshots by hashing the already-built header only, and derive the
        // cumulative wire commitment from the retained flashblocks for child rollovers.
        let latest_header = Self::hash_existing_header(suffix_header);
        pending_block.latest_wire_header_hash = latest_wire_header_hash;
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
        if block.number != pending_block.block_number {
            return false;
        }

        if block.header().parent_hash != pending_block.parent_hash {
            return false;
        }

        let pending_tx_hashes = pending_block
            .transactions
            .iter()
            .map(|transaction| transaction.hash)
            .collect::<Vec<_>>();
        let canonical_tx_hashes =
            block.body().transactions().map(|tx| tx.tx_hash()).collect::<Vec<_>>();

        pending_tx_hashes == canonical_tx_hashes
    }

    /// Wraps an existing header with `hash_slow()` only.
    ///
    /// This does not derive `state_root` or any other header parts.
    fn hash_existing_header(header: Header) -> Sealed<Header> {
        let hash = header.hash_slow();
        Sealed::new_unchecked(header, hash)
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
    use reth_primitives_traits::RecoveredBlock;
    use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
    use reth_storage_api::HeaderProvider;

    use super::{
        FlashblockPrefixMismatch, HotApplyOutcome, HotEngine, HotInvalidationReason,
        HotPendingBlock, RetainedOutputPrefixMismatch,
    };
    use crate::{
        BlockAssembler, HotWindowAnchor, PeriodicAuditFailure, PeriodicAuditResult,
        RetainedFastOutput, ShadowRebuildCompletion,
        periodic_audit::{PeriodicAuditOutputMismatch, PeriodicAuditOutputMismatchField},
    };

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
                gas_used: 100_000u64.saturating_mul(index + 1),
                block_hash: fixture_wire_block_hash(block_number, index),
                transactions: transactions
                    .into_iter()
                    .map(|transaction| transaction.encoded_2718().into())
                    .collect(),
                withdrawals: vec![],
                withdrawals_root: B256::ZERO,
                blob_gas_used: None,
            },
            metadata: Metadata::new(block_number),
        }
    }

    fn fixture_wire_block_hash(block_number: u64, index: u64) -> B256 {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&block_number.to_be_bytes());
        bytes[8..16].copy_from_slice(&index.to_be_bytes());
        bytes[31] = 1;
        B256::from(bytes)
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

    fn canonical_block_from_pending_block(
        pending_block: &HotPendingBlock,
    ) -> RecoveredBlock<BaseBlock> {
        let header = pending_block.latest_header.inner().clone();
        let transactions = pending_block
            .flashblocks
            .iter()
            .flat_map(|flashblock| {
                BlockAssembler::decode_flashblock_transactions(flashblock)
                    .expect("pending flashblock transactions should decode")
            })
            .collect::<Vec<_>>();

        canonical_block_with_header(header, transactions)
    }

    fn insert_canonical_header(
        client: &MockEthProvider<BasePrimitives, Arc<BaseChainSpec>>,
        block: &RecoveredBlock<BaseBlock>,
    ) {
        client.add_header(block.header().hash_slow(), block.header().clone());
    }

    fn active_pending_block_wire_hash(
        engine: &HotEngine<MockEthProvider<BasePrimitives, Arc<BaseChainSpec>>>,
    ) -> B256 {
        engine.window.active_block().expect("active block should exist").latest_wire_header_hash
    }

    fn cumulative_wire_hash(flashblocks: &[Flashblock]) -> B256 {
        BlockAssembler::assemble(flashblocks)
            .expect("test flashblocks should assemble")
            .block
            .header
            .hash_slow()
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
            vec![decoded_l1_info_tx(), deploy_tx],
        );

        let HotApplyOutcome::Delta {
            delta: first_delta,
            ready_cached_block: first_ready_cached_block,
            ..
        } = engine.apply_flashblock(&first_flashblock).expect("first flashblock should apply")
        else {
            panic!("expected first flashblock delta outcome");
        };
        assert_eq!(first_ready_cached_block, Some(1));
        assert_eq!(first_delta.logs.len(), 1);
        assert_eq!(first_delta.logs[0].tx_index, 1);
        assert_eq!(first_delta.logs[0].log_index_in_block, 0);

        let second_flashblock =
            flashblock(1, 1, PayloadId::new([0x11; 8]), parent_hash, false, vec![call_tx.clone()]);

        let HotApplyOutcome::Delta {
            delta: second_delta,
            ready_cached_block: second_ready_cached_block,
            ..
        } = engine.apply_flashblock(&second_flashblock).expect("same-block suffix should apply")
        else {
            panic!("expected same-block delta outcome");
        };
        assert_eq!(second_ready_cached_block, None);

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
        assert_eq!(active_block.published_outputs.len(), 2);
        assert_eq!(active_block.published_outputs[0], RetainedFastOutput::from_delta(&first_delta));
        assert_eq!(
            active_block.published_outputs[1],
            RetainedFastOutput::from_delta(&second_delta)
        );
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
            vec![decoded_l1_info_tx(), deploy_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first block should apply");

        let carried_parent_hash = active_pending_block_wire_hash(&engine);
        let second_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0x22; 8]),
            carried_parent_hash,
            true,
            vec![decoded_l1_info_tx(), call_tx.clone()],
        );

        let HotApplyOutcome::Delta { delta, ready_cached_block, .. } =
            engine.apply_flashblock(&second_flashblock).expect("next block should roll over")
        else {
            panic!("expected next-block delta outcome");
        };

        assert_eq!(ready_cached_block, Some(2));

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
    fn hot_engine_rollover_marks_accepted_block_ready_for_cached_suffixes() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();

        let mut engine = HotEngine::new(client, 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x71; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx()],
        );
        engine.apply_flashblock(&first_flashblock).expect("first block should apply");

        let second_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0x72; 8]),
            active_pending_block_wire_hash(&engine),
            true,
            vec![decoded_l1_info_tx()],
        );

        let HotApplyOutcome::Delta { ready_cached_block, .. } =
            engine.apply_flashblock(&second_flashblock).expect("rollover should apply")
        else {
            panic!("expected rollover delta outcome");
        };

        assert_eq!(ready_cached_block, Some(2));
    }

    #[test]
    fn hot_engine_rollover_accepts_cumulative_wire_parent_commitment_without_local_state_root() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_deploy_tx = create_deploy_log_tx(0x91);
        let first_block_contract =
            first_block_deploy_tx.recover_signer().expect("deploy signer should recover").create(0);
        let first_block_call_tx = create_call_log_tx(first_block_contract, 0x92);
        let second_block_deploy_tx = create_deploy_log_tx_with_gas_limit(0x93, 120_000);
        seed_sender_balance(&client, &first_block_deploy_tx);
        seed_sender_balance(&client, &first_block_call_tx);
        seed_sender_balance(&client, &second_block_deploy_tx);

        let mut engine = HotEngine::new(client, 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x81; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), first_block_deploy_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first flashblock should apply");

        let second_flashblock = flashblock(
            1,
            1,
            PayloadId::new([0x81; 8]),
            canonical_parent_hash,
            false,
            vec![first_block_call_tx],
        );
        engine.apply_flashblock(&second_flashblock).expect("second flashblock should apply");

        let cumulative_wire_parent_hash =
            cumulative_wire_hash(&[first_flashblock.clone(), second_flashblock.clone()]);
        assert_ne!(cumulative_wire_parent_hash, second_flashblock.diff.block_hash);

        let wire_parent_hash = {
            let active_block = engine.window.active_block().expect("active block should exist");
            assert!(active_block.local_header_parts.is_none());
            assert_eq!(active_block.latest_wire_header_hash, cumulative_wire_parent_hash);
            active_block.latest_wire_header_hash
        };
        let third_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0x83; 8]),
            wire_parent_hash,
            true,
            vec![decoded_l1_info_tx(), second_block_deploy_tx],
        );

        let outcome = engine.apply_flashblock(&third_flashblock).expect("rollover should apply");

        assert!(matches!(outcome, HotApplyOutcome::Delta { .. }));
        assert_eq!(engine.window.active_block_number(), Some(2));
        assert!(
            engine
                .window
                .blocks
                .front()
                .expect("previous block should remain in window")
                .local_header_parts
                .is_none()
        );
    }

    #[test]
    fn hot_engine_rollover_accepts_cumulative_wire_parent_hash_across_suffixes() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_deploy_tx = create_deploy_log_tx(0x9a);
        let first_block_contract =
            first_block_deploy_tx.recover_signer().expect("deploy signer should recover").create(0);
        let first_block_call_tx = create_call_log_tx(first_block_contract, 0x9b);
        let second_block_deploy_tx = create_deploy_log_tx_with_gas_limit(0x9c, 120_000);
        seed_sender_balance(&client, &first_block_deploy_tx);
        seed_sender_balance(&client, &first_block_call_tx);
        seed_sender_balance(&client, &second_block_deploy_tx);

        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x8a; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), first_block_deploy_tx],
        );
        let second_flashblock = flashblock(
            1,
            1,
            PayloadId::new([0x8a; 8]),
            canonical_parent_hash,
            false,
            vec![first_block_call_tx],
        );
        let expected_wire_parent_hash =
            cumulative_wire_hash(&[first_flashblock.clone(), second_flashblock.clone()]);
        assert_ne!(expected_wire_parent_hash, second_flashblock.diff.block_hash);

        let mut engine = HotEngine::new(client, 3);
        engine.apply_flashblock(&first_flashblock).expect("first flashblock should apply");
        engine.apply_flashblock(&second_flashblock).expect("second flashblock should apply");

        assert_eq!(active_pending_block_wire_hash(&engine), expected_wire_parent_hash);

        let outcome = engine
            .apply_flashblock(&flashblock(
                0,
                2,
                PayloadId::new([0x8b; 8]),
                expected_wire_parent_hash,
                true,
                vec![decoded_l1_info_tx(), second_block_deploy_tx],
            ))
            .expect("rollover should accept the cumulative wire parent hash");

        assert!(matches!(outcome, HotApplyOutcome::Delta { .. }));
        assert_eq!(engine.window.active_block_number(), Some(2));
    }

    #[test]
    fn hot_engine_same_block_zero_wire_commitment_invalidates_session() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let deploy_tx = create_deploy_log_tx(0x90);
        seed_sender_balance(&client, &deploy_tx);

        let mut engine = HotEngine::new(client, 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x80; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), deploy_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first flashblock should apply");

        let mut malformed_suffix =
            flashblock(1, 1, PayloadId::new([0x80; 8]), canonical_parent_hash, false, vec![]);
        malformed_suffix.diff.block_hash = B256::ZERO;

        let outcome = engine
            .apply_flashblock(&malformed_suffix)
            .expect("malformed same-block suffix should invalidate the active session");

        assert!(matches!(
            outcome,
            HotApplyOutcome::InvalidateSession {
                reason: HotInvalidationReason::ContinuityViolation
            }
        ));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_rollover_missing_base_invalidates_active_session() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let deploy_tx = create_deploy_log_tx(0x93);
        seed_sender_balance(&client, &deploy_tx);

        let mut engine = HotEngine::new(client, 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x83; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), deploy_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first flashblock should apply");

        let outcome = engine
            .apply_flashblock(&flashblock(
                0,
                2,
                PayloadId::new([0x84; 8]),
                canonical_parent_hash,
                false,
                vec![],
            ))
            .expect("missing rollover base should invalidate the active session");

        assert!(matches!(
            outcome,
            HotApplyOutcome::InvalidateSession {
                reason: HotInvalidationReason::ContinuityViolation
            }
        ));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_rollover_accepts_child_parent_without_local_wire_validation() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0x94);
        let second_block_tx = create_deploy_log_tx_with_gas_limit(0x95, 120_000);
        seed_sender_balance(&client, &first_block_tx);
        seed_sender_balance(&client, &second_block_tx);

        let mut engine = HotEngine::new(client.clone(), 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x84; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), first_block_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first flashblock should apply");

        let canonical_block = canonical_block_with_header(
            Header {
                number: 1,
                parent_hash: canonical_parent_hash,
                extra_data: Bytes::from_static(b"canonical-parent"),
                ..Default::default()
            },
            vec![],
        );
        insert_canonical_header(&client, &canonical_block);

        let unvalidated_parent_hash = B256::with_last_byte(0x99);
        let HotApplyOutcome::Delta { delta, .. } = engine
            .apply_flashblock(&flashblock(
                0,
                2,
                PayloadId::new([0x85; 8]),
                unvalidated_parent_hash,
                true,
                vec![decoded_l1_info_tx(), second_block_tx],
            ))
            .expect("rollover should accept the child parent hash without local validation")
        else {
            panic!("expected rollover delta outcome");
        };

        assert_eq!(delta.parent_hash, unvalidated_parent_hash);
        assert_eq!(engine.window.active_block_number(), Some(2));
        assert_eq!(
            engine.window.active_block().expect("child block should exist").parent_hash,
            unvalidated_parent_hash
        );
    }

    #[test]
    fn hot_engine_rollover_parent_hash_mismatch_still_advances_child() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let deploy_tx = create_deploy_log_tx(0x98);
        seed_sender_balance(&client, &deploy_tx);

        let mut engine = HotEngine::new(client, 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x87; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), deploy_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first flashblock should apply");

        let unvalidated_parent_hash = B256::with_last_byte(0xfe);
        let HotApplyOutcome::Delta { delta, .. } = engine
            .apply_flashblock(&flashblock(
                0,
                2,
                PayloadId::new([0x88; 8]),
                unvalidated_parent_hash,
                true,
                vec![decoded_l1_info_tx()],
            ))
            .expect("rollover should still advance the child block")
        else {
            panic!("expected rollover delta outcome");
        };

        assert_eq!(delta.parent_hash, unvalidated_parent_hash);
        assert_eq!(engine.window.active_block_number(), Some(2));
        assert_eq!(engine.window.blocks.len(), 2);
        assert!(engine.window.execution.is_some());
    }

    #[test]
    fn hot_engine_rollover_accepts_previous_suffix_block_hash_when_cumulative_wire_hash_differs() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_deploy_tx = create_deploy_log_tx(0x36);
        let first_block_contract =
            first_block_deploy_tx.recover_signer().expect("deploy signer should recover").create(0);
        let first_block_call_tx = create_call_log_tx(first_block_contract, 0x37);
        let second_block_deploy_tx = create_deploy_log_tx_with_gas_limit(0x38, 120_000);
        seed_sender_balance(&client, &first_block_deploy_tx);
        seed_sender_balance(&client, &first_block_call_tx);
        seed_sender_balance(&client, &second_block_deploy_tx);

        let mut engine = HotEngine::new(client, 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x26; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), first_block_deploy_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first flashblock should apply");

        let second_flashblock = flashblock(
            1,
            1,
            PayloadId::new([0x26; 8]),
            canonical_parent_hash,
            false,
            vec![first_block_call_tx],
        );
        engine.apply_flashblock(&second_flashblock).expect("second flashblock should apply");

        let cumulative_wire_parent_hash =
            cumulative_wire_hash(&[first_flashblock.clone(), second_flashblock.clone()]);
        let suffix_block_hash = second_flashblock.diff.block_hash;
        assert_ne!(cumulative_wire_parent_hash, suffix_block_hash);

        let third_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0x28; 8]),
            suffix_block_hash,
            true,
            vec![decoded_l1_info_tx(), second_block_deploy_tx],
        );

        let HotApplyOutcome::Delta { delta, .. } = engine
            .apply_flashblock(&third_flashblock)
            .expect("rollover should accept a real boundary suffix block hash regression case")
        else {
            panic!("expected rollover delta outcome");
        };

        assert_eq!(delta.parent_hash, suffix_block_hash);
        assert_eq!(engine.window.active_block_number(), Some(2));
        assert_eq!(
            engine.window.active_block().expect("child block should exist").parent_hash,
            suffix_block_hash
        );
    }

    #[test]
    fn hot_engine_same_block_payload_id_change_invalidates_session() {
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

        let outcome = engine
            .apply_flashblock(&flashblock(
                1,
                1,
                PayloadId::new([0x32; 8]),
                parent_hash,
                false,
                vec![create_deploy_log_tx(0x42)],
            ))
            .expect("payload change should invalidate the active session");

        assert!(matches!(
            outcome,
            HotApplyOutcome::InvalidateSession {
                reason: HotInvalidationReason::ContinuityViolation
            }
        ));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_same_block_base_field_change_invalidates_session() {
        let client = test_client();
        let parent_hash = client.chain_spec().genesis_hash();
        let deploy_tx = create_deploy_log_tx(0x43);
        seed_sender_balance(&client, &deploy_tx);

        let mut engine = HotEngine::new(client, 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x33; 8]),
            parent_hash,
            true,
            vec![decoded_l1_info_tx(), deploy_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first flashblock should apply");

        let mut changed_base_flashblock = flashblock(
            1,
            1,
            PayloadId::new([0x33; 8]),
            parent_hash,
            true,
            vec![create_deploy_log_tx(0x44)],
        );
        changed_base_flashblock
            .base
            .as_mut()
            .expect("same-block test flashblock should carry base")
            .extra_data = Bytes::from_static(b"changed-base");

        let outcome = engine
            .apply_flashblock(&changed_base_flashblock)
            .expect("base field change should invalidate the active session");

        assert!(matches!(
            outcome,
            HotApplyOutcome::InvalidateSession {
                reason: HotInvalidationReason::ContinuityViolation
            }
        ));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_duplicate_same_block_diff_change_invalidates_session() {
        let client = test_client();
        let parent_hash = client.chain_spec().genesis_hash();
        let deploy_tx = create_deploy_log_tx(0x45);
        seed_sender_balance(&client, &deploy_tx);

        let mut engine = HotEngine::new(client, 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x35; 8]),
            parent_hash,
            true,
            vec![decoded_l1_info_tx(), deploy_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first flashblock should apply");

        let mut changed_duplicate = first_flashblock.clone();
        changed_duplicate.diff.gas_used = changed_duplicate.diff.gas_used.saturating_add(1);

        let outcome = engine
            .apply_flashblock(&changed_duplicate)
            .expect("duplicate diff change should invalidate the active session");

        assert!(matches!(
            outcome,
            HotApplyOutcome::InvalidateSession {
                reason: HotInvalidationReason::ContinuityViolation
            }
        ));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_duplicate_same_block_transaction_change_invalidates_session() {
        let client = test_client();
        let parent_hash = client.chain_spec().genesis_hash();
        let deploy_tx = create_deploy_log_tx(0x46);
        seed_sender_balance(&client, &deploy_tx);

        let mut engine = HotEngine::new(client, 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x36; 8]),
            parent_hash,
            true,
            vec![decoded_l1_info_tx(), deploy_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first flashblock should apply");

        let changed_duplicate = flashblock(
            0,
            1,
            PayloadId::new([0x36; 8]),
            parent_hash,
            true,
            vec![decoded_l1_info_tx(), create_deploy_log_tx_with_gas_limit(0x47, 120_000)],
        );

        let outcome = engine
            .apply_flashblock(&changed_duplicate)
            .expect("duplicate transaction change should invalidate the active session");

        assert!(matches!(
            outcome,
            HotApplyOutcome::InvalidateSession {
                reason: HotInvalidationReason::ContinuityViolation
            }
        ));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_gap_invalidates_active_session() {
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
            engine.apply_flashblock(&gap_flashblock).expect("gap flashblock should invalidate");

        assert!(matches!(
            outcome,
            HotApplyOutcome::InvalidateSession {
                reason: HotInvalidationReason::ContinuityViolation
            }
        ));
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
    fn hot_engine_anchor_canonical_update_is_duplicate_without_legacy_replay() {
        let client = test_client();
        let parent_hash = client.chain_spec().genesis_hash();
        let deploy_tx = create_deploy_log_tx(0x5b);
        let call_tx = create_call_log_tx(
            deploy_tx.recover_signer().expect("deploy signer should recover").create(0),
            0x5c,
        );
        seed_sender_balance(&client, &deploy_tx);
        seed_sender_balance(&client, &call_tx);

        let mut engine = HotEngine::new(client.clone(), 3);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0x5b; 8]),
                parent_hash,
                true,
                vec![decoded_l1_info_tx(), deploy_tx],
            ))
            .expect("first flashblock should apply");
        engine
            .apply_flashblock(&flashblock(
                1,
                1,
                PayloadId::new([0x5b; 8]),
                parent_hash,
                false,
                vec![call_tx],
            ))
            .expect("same-block suffix should apply");

        let genesis_header = client
            .header_by_number(0)
            .expect("genesis header lookup should succeed")
            .expect("genesis header should exist");
        let genesis_block = canonical_block_with_header(genesis_header, vec![]);

        let outcome = engine
            .process_canonical_block(&genesis_block)
            .expect("matching canonical anchor should be ignored without replay");

        assert!(matches!(outcome, HotApplyOutcome::Duplicate));
        assert_eq!(engine.window.anchor.block_number(), 0);
        assert_eq!(engine.window.anchor.hash(), parent_hash);
        assert!(engine.window.execution.is_some());
        assert_eq!(engine.window.blocks.len(), 1);
        assert_eq!(engine.window.blocks.front().map(|block| block.block_number), Some(1));
        assert_eq!(
            engine.window.latest_audit_cursor().map(|cursor| cursor.flashblock_index),
            Some(1)
        );
    }

    #[test]
    fn hot_engine_canonical_match_prunes_oldest_block_when_child_parent_matches_canonical_hash() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0x5d);
        let second_block_tx = create_deploy_log_tx_with_gas_limit(0x5e, 120_000);
        seed_sender_balance(&client, &first_block_tx);
        seed_sender_balance(&client, &second_block_tx);

        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x5d; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), first_block_tx],
        );

        let mut engine = HotEngine::new(client, 3);
        engine.apply_flashblock(&first_flashblock).expect("first block should apply");
        let canonical_block = canonical_block_from_pending_block(
            engine.window.active_block().expect("first retained block should exist"),
        );
        let canonical_hash = canonical_block.header().hash_slow();

        let second_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0x5e; 8]),
            active_pending_block_wire_hash(&engine),
            true,
            vec![decoded_l1_info_tx(), second_block_tx],
        );

        engine.apply_flashblock(&second_flashblock).expect("second block should apply");
        assert_eq!(
            engine.window.blocks.front().expect("first retained block should exist").parent_hash,
            canonical_parent_hash
        );
        assert_eq!(
            engine
                .window
                .blocks
                .front()
                .expect("first retained block should exist")
                .transactions
                .iter()
                .map(|transaction| transaction.hash)
                .collect::<Vec<_>>(),
            canonical_block.body().transactions().map(|tx| tx.tx_hash()).collect::<Vec<_>>()
        );
        assert_eq!(engine.window.blocks.len(), 2);
        assert_eq!(
            engine.window.blocks.get(1).expect("second retained block should exist").parent_hash,
            canonical_hash
        );

        insert_canonical_header(&engine.client, &canonical_block);

        let outcome = engine
            .process_canonical_block(&canonical_block)
            .expect("matching canonical block should prune the oldest retained block");

        assert!(
            matches!(outcome, HotApplyOutcome::CanonicalWindowChanged),
            "unexpected canonical outcome: {outcome:?}"
        );
        assert_eq!(engine.window.anchor, HotWindowAnchor::new(1, canonical_hash));
        assert!(engine.window.execution.is_some());
        assert_eq!(engine.window.blocks.len(), 1);
        assert_eq!(engine.window.blocks.front().map(|block| block.block_number), Some(2));
        assert_eq!(
            engine.window.active_block().expect("second block should remain active").parent_hash,
            canonical_hash
        );
    }

    #[test]
    fn hot_engine_canonical_rebase_replaces_live_execution_with_rebuilt_suffix_state() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0x5f);
        let second_block_tx = create_deploy_log_tx_with_gas_limit(0x60, 120_000);
        seed_sender_balance(&client, &first_block_tx);
        seed_sender_balance(&client, &second_block_tx);

        let mut engine = HotEngine::new(client.clone(), 3);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0x5f; 8]),
                canonical_parent_hash,
                true,
                vec![decoded_l1_info_tx(), first_block_tx],
            ))
            .expect("first block should apply");
        engine
            .apply_flashblock(&flashblock(
                0,
                2,
                PayloadId::new([0x60; 8]),
                active_pending_block_wire_hash(&engine),
                true,
                vec![decoded_l1_info_tx(), second_block_tx],
            ))
            .expect("second block should apply");

        let canonical_block = canonical_block_from_pending_block(
            engine.window.blocks.front().expect("first retained block should exist"),
        );
        let canonical_hash = canonical_block.header().hash_slow();
        insert_canonical_header(&client, &canonical_block);

        let snapshot = engine
            .retained_suffix_snapshot_after_canonical_anchor(canonical_block.number, canonical_hash)
            .expect("retained suffix should snapshot");
        let (audit_result, rebuilt_engine) =
            HotEngine::rebuild_shadow_from_snapshot(client, 3, &snapshot);
        assert_eq!(audit_result, PeriodicAuditResult::EquivalentPrefix);
        let rebuilt_engine = rebuilt_engine.expect("matching rebase should rebuild suffix state");

        let stale_execution =
            HotEngine::<MockEthProvider<BasePrimitives, Arc<BaseChainSpec>>>::hash_existing_header(
                Header {
                    number: 99,
                    parent_hash: B256::with_last_byte(0xfe),
                    ..Default::default()
                },
            );
        let stale_execution_hash = stale_execution.hash();
        engine.window.execution.as_mut().expect("live execution should exist").last_header =
            stale_execution;

        let outcome = engine
            .process_canonical_block(&canonical_block)
            .expect("matching canonical block should rebase the retained suffix");

        assert!(matches!(outcome, HotApplyOutcome::CanonicalWindowChanged));
        assert_eq!(engine.window.anchor, HotWindowAnchor::new(1, canonical_hash));
        assert_eq!(engine.window.blocks.len(), 1);
        assert_eq!(engine.window.blocks.front().map(|block| block.block_number), Some(2));
        assert_ne!(
            engine
                .window
                .execution
                .as_ref()
                .expect("rebased execution should exist")
                .last_header
                .hash(),
            stale_execution_hash
        );
        assert_eq!(
            engine
                .window
                .execution
                .as_ref()
                .expect("rebased execution should exist")
                .last_header
                .hash(),
            rebuilt_engine
                .window
                .execution
                .as_ref()
                .expect("rebuilt execution should exist")
                .last_header
                .hash()
        );
    }

    #[test]
    fn hot_engine_canonical_rebase_preserves_continuity_for_next_suffix() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();

        let mut engine = HotEngine::new(client.clone(), 3);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0x61; 8]),
                canonical_parent_hash,
                true,
                vec![decoded_l1_info_tx()],
            ))
            .expect("first block should apply");
        let second_payload_id = PayloadId::new([0x62; 8]);
        engine
            .apply_flashblock(&flashblock(
                0,
                2,
                second_payload_id,
                active_pending_block_wire_hash(&engine),
                true,
                vec![decoded_l1_info_tx()],
            ))
            .expect("second block should apply");

        let canonical_block = canonical_block_from_pending_block(
            engine.window.blocks.front().expect("first retained block should exist"),
        );
        let canonical_hash = canonical_block.header().hash_slow();
        insert_canonical_header(&client, &canonical_block);

        let snapshot = engine
            .retained_suffix_snapshot_after_canonical_anchor(canonical_block.number, canonical_hash)
            .expect("retained suffix should snapshot");
        let (audit_result, rebuilt_engine) =
            HotEngine::rebuild_shadow_from_snapshot(client, 3, &snapshot);
        assert_eq!(audit_result, PeriodicAuditResult::EquivalentPrefix);
        let mut rebuilt_engine = rebuilt_engine.expect("matching rebase should rebuild suffix");

        let outcome = engine
            .process_canonical_block(&canonical_block)
            .expect("matching canonical block should preserve continuity");
        assert!(matches!(outcome, HotApplyOutcome::CanonicalWindowChanged));

        let next_suffix = flashblock(1, 2, second_payload_id, canonical_hash, false, vec![]);
        let HotApplyOutcome::Delta { delta: expected_delta, .. } = rebuilt_engine
            .apply_flashblock(&next_suffix)
            .expect("rebuilt suffix should accept the next same-block flashblock")
        else {
            panic!("expected rebuilt suffix delta outcome");
        };
        let HotApplyOutcome::Delta { delta: live_delta, .. } = engine
            .apply_flashblock(&next_suffix)
            .expect("rebased live suffix should accept the next same-block flashblock")
        else {
            panic!("expected live suffix delta outcome");
        };

        assert_eq!(
            RetainedFastOutput::from_delta(&live_delta),
            RetainedFastOutput::from_delta(&expected_delta)
        );
        assert_eq!(engine.window.anchor, HotWindowAnchor::new(1, canonical_hash));
        assert_eq!(engine.window.active_block_number(), Some(2));
        assert_eq!(
            engine
                .window
                .active_block()
                .expect("rebased active block should exist")
                .latest_flashblock_index,
            1
        );
    }

    #[test]
    fn hot_engine_canonical_rebase_semantic_drift_rebuilds_retained_suffix() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();

        let mut engine = HotEngine::new(client.clone(), 3);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0x63; 8]),
                canonical_parent_hash,
                true,
                vec![decoded_l1_info_tx()],
            ))
            .expect("first block should apply");
        engine
            .apply_flashblock(&flashblock(
                0,
                2,
                PayloadId::new([0x64; 8]),
                active_pending_block_wire_hash(&engine),
                true,
                vec![decoded_l1_info_tx()],
            ))
            .expect("second block should apply");

        let canonical_block = canonical_block_from_pending_block(
            engine.window.blocks.front().expect("first retained block should exist"),
        );
        let canonical_hash = canonical_block.header().hash_slow();
        insert_canonical_header(&client, &canonical_block);

        let snapshot = engine
            .retained_suffix_snapshot_after_canonical_anchor(canonical_block.number, canonical_hash)
            .expect("retained suffix should snapshot");
        let (audit_result, rebuilt_engine) =
            HotEngine::rebuild_shadow_from_snapshot(client, 3, &snapshot);
        assert_eq!(audit_result, PeriodicAuditResult::EquivalentPrefix);
        let rebuilt_engine = rebuilt_engine.expect("matching rebase should rebuild suffix state");
        let expected_output = rebuilt_engine
            .window
            .blocks
            .front()
            .expect("rebuilt suffix block should exist")
            .published_outputs[0]
            .clone();

        engine
            .window
            .blocks
            .get_mut(1)
            .expect("second retained block should exist")
            .published_outputs[0]
            .block_timestamp = Some(1_234_567_890);

        let outcome = engine
            .process_canonical_block(&canonical_block)
            .expect("rebase should rebuild retained suffix from canonical anchor");

        assert!(matches!(outcome, HotApplyOutcome::CanonicalWindowChanged));
        assert_eq!(engine.window.anchor, HotWindowAnchor::new(1, canonical_hash));
        assert!(engine.window.execution.is_some());
        assert_eq!(engine.window.blocks.len(), 1);
        assert_eq!(engine.window.blocks.front().map(|block| block.block_number), Some(2));
        assert_eq!(
            engine
                .window
                .blocks
                .front()
                .expect("retained suffix block should remain")
                .published_outputs[0],
            expected_output
        );
        assert_ne!(
            engine
                .window
                .blocks
                .front()
                .expect("retained suffix block should remain")
                .published_outputs[0]
                .block_timestamp,
            Some(1_234_567_890)
        );
    }

    #[test]
    fn hot_engine_canonical_rebase_structural_mismatch_invalidates_session() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();

        let mut engine = HotEngine::new(client.clone(), 3);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0x63; 8]),
                canonical_parent_hash,
                true,
                vec![decoded_l1_info_tx()],
            ))
            .expect("first block should apply");
        engine
            .apply_flashblock(&flashblock(
                0,
                2,
                PayloadId::new([0x64; 8]),
                active_pending_block_wire_hash(&engine),
                true,
                vec![decoded_l1_info_tx()],
            ))
            .expect("second block should apply");

        let canonical_block = canonical_block_from_pending_block(
            engine.window.blocks.front().expect("first retained block should exist"),
        );
        insert_canonical_header(&client, &canonical_block);

        engine
            .window
            .blocks
            .get_mut(1)
            .expect("second retained block should exist")
            .flashblocks
            .first_mut()
            .expect("retained suffix should include its first flashblock")
            .base
            .as_mut()
            .expect("retained suffix should keep its first flashblock base")
            .parent_hash = B256::with_last_byte(0xee);

        let outcome = engine
            .process_canonical_block(&canonical_block)
            .expect("rebase structural mismatch should fail closed");

        assert!(matches!(
            outcome,
            HotApplyOutcome::InvalidateSession {
                reason: HotInvalidationReason::UnrecoverableReplayFailure
            }
        ));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_canonical_catchup_without_retained_audit_window_resets() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0x52);
        let second_block_tx = create_deploy_log_tx_with_gas_limit(0x53, 110_000);
        let third_block_tx = create_deploy_log_tx_with_gas_limit(0x54, 120_000);
        seed_sender_balance(&client, &first_block_tx);
        seed_sender_balance(&client, &second_block_tx);
        seed_sender_balance(&client, &third_block_tx);

        let mut engine = HotEngine::new(client.clone(), 4);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0x42; 8]),
                canonical_parent_hash,
                true,
                vec![decoded_l1_info_tx(), first_block_tx],
            ))
            .expect("first block should apply");
        let second_parent_hash = active_pending_block_wire_hash(&engine);
        engine
            .apply_flashblock(&flashblock(
                0,
                2,
                PayloadId::new([0x43; 8]),
                second_parent_hash,
                true,
                vec![decoded_l1_info_tx(), second_block_tx],
            ))
            .expect("second block should apply");
        let third_parent_hash = active_pending_block_wire_hash(&engine);
        engine
            .apply_flashblock(&flashblock(
                0,
                3,
                PayloadId::new([0x44; 8]),
                third_parent_hash,
                true,
                vec![decoded_l1_info_tx(), third_block_tx],
            ))
            .expect("third block should apply");
        let first_canonical_block = canonical_block_from_pending_block(
            engine.window.blocks.front().expect("first retained block should exist"),
        );
        let second_canonical_block = canonical_block_from_pending_block(
            engine.window.blocks.get(1).expect("second retained block should exist"),
        );
        let canonical_catchup_block = canonical_block_from_pending_block(
            engine.window.active_block().expect("canonical catch-up block should exist"),
        );
        insert_canonical_header(&client, &first_canonical_block);
        insert_canonical_header(&client, &second_canonical_block);
        insert_canonical_header(&client, &canonical_catchup_block);

        let outcome = engine.process_canonical_block(&canonical_catchup_block).expect(
            "matching canonical catch-up should reset when no retained audit window exists",
        );

        assert!(matches!(outcome, HotApplyOutcome::Reset));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_canonical_catchup_without_retained_audit_window_resets_even_with_intermediate_conflicts()
     {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0x55);
        let second_block_tx = create_deploy_log_tx_with_gas_limit(0x56, 110_000);
        let third_block_tx = create_deploy_log_tx_with_gas_limit(0x57, 120_000);
        seed_sender_balance(&client, &first_block_tx);
        seed_sender_balance(&client, &second_block_tx);
        seed_sender_balance(&client, &third_block_tx);

        let mut engine = HotEngine::new(client.clone(), 4);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0x45; 8]),
                canonical_parent_hash,
                true,
                vec![decoded_l1_info_tx(), first_block_tx],
            ))
            .expect("first block should apply");
        let second_parent_hash = active_pending_block_wire_hash(&engine);
        engine
            .apply_flashblock(&flashblock(
                0,
                2,
                PayloadId::new([0x46; 8]),
                second_parent_hash,
                true,
                vec![decoded_l1_info_tx(), second_block_tx],
            ))
            .expect("second block should apply");
        let third_parent_hash = active_pending_block_wire_hash(&engine);
        engine
            .apply_flashblock(&flashblock(
                0,
                3,
                PayloadId::new([0x47; 8]),
                third_parent_hash,
                true,
                vec![decoded_l1_info_tx(), third_block_tx],
            ))
            .expect("third block should apply");

        let first_canonical_block = canonical_block_from_pending_block(
            engine.window.blocks.front().expect("first retained block should exist"),
        );
        let conflicting_second_canonical_block = canonical_block_with_header(
            Header {
                number: 2,
                parent_hash: first_canonical_block.header().hash_slow(),
                extra_data: Bytes::from_static(b"catchup-canonical-conflict"),
                ..Default::default()
            },
            vec![],
        );
        let canonical_catchup_block = canonical_block_with_header(
            Header {
                number: 3,
                parent_hash: conflicting_second_canonical_block.header().hash_slow(),
                extra_data: Bytes::from_static(b"catchup-tip"),
                ..Default::default()
            },
            vec![],
        );
        insert_canonical_header(&client, &first_canonical_block);
        insert_canonical_header(&client, &conflicting_second_canonical_block);
        insert_canonical_header(&client, &canonical_catchup_block);

        let outcome = engine
            .process_canonical_block(&canonical_catchup_block)
            .expect("catch-up without retained audit window should reset even across conflicts");

        assert!(matches!(outcome, HotApplyOutcome::Reset));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_canonical_same_number_multiflashblock_resets_when_child_keeps_wire_parent() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_deploy_tx = create_deploy_log_tx(0x61);
        let first_block_contract =
            first_block_deploy_tx.recover_signer().expect("deploy signer should recover").create(0);
        let first_block_call_tx = create_call_log_tx(first_block_contract, 0x62);
        let second_block_deploy_tx = create_deploy_log_tx_with_gas_limit(0x63, 120_000);
        seed_sender_balance(&client, &first_block_deploy_tx);
        seed_sender_balance(&client, &first_block_call_tx);
        seed_sender_balance(&client, &second_block_deploy_tx);

        let mut engine = HotEngine::new(client.clone(), 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x51; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), first_block_deploy_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first block should apply");

        let second_flashblock = flashblock(
            1,
            1,
            PayloadId::new([0x51; 8]),
            canonical_parent_hash,
            false,
            vec![first_block_call_tx],
        );
        engine.apply_flashblock(&second_flashblock).expect("second block should apply");

        let carried_parent_hash = active_pending_block_wire_hash(&engine);
        let canonical_block = canonical_block_from_pending_block(
            engine.window.active_block().expect("active block should exist"),
        );
        let third_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0x53; 8]),
            carried_parent_hash,
            true,
            vec![decoded_l1_info_tx(), second_block_deploy_tx],
        );
        engine.apply_flashblock(&third_flashblock).expect("third block should apply");

        insert_canonical_header(&client, &canonical_block);

        let outcome = engine.process_canonical_block(&canonical_block).expect(
            "canonical block should reset when the retained child still points at the wire parent",
        );

        assert!(matches!(outcome, HotApplyOutcome::Reset));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_canonical_same_number_prunes_when_child_keeps_cumulative_wire_parent() {
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
            vec![decoded_l1_info_tx(), deploy_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first block should apply");

        let carried_parent_hash = active_pending_block_wire_hash(&engine);
        let second_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0x62; 8]),
            carried_parent_hash,
            true,
            vec![decoded_l1_info_tx(), next_block_tx],
        );
        engine.apply_flashblock(&second_flashblock).expect("second block should apply");

        let canonical_block = canonical_block_from_pending_block(
            engine.window.blocks.front().expect("first pending block should exist"),
        );
        insert_canonical_header(&client, &canonical_block);

        let outcome = engine
            .process_canonical_block(&canonical_block)
            .expect("matching canonical block should prune when the retained child points at the cumulative wire parent");

        assert!(matches!(outcome, HotApplyOutcome::CanonicalWindowChanged));
        assert_eq!(
            engine.window.anchor,
            HotWindowAnchor::new(1, canonical_block.header().hash_slow())
        );
        assert!(engine.window.execution.is_some());
        assert_eq!(engine.window.blocks.len(), 1);
        assert_eq!(engine.window.active_block_number(), Some(2));
    }

    #[test]
    fn hot_engine_matching_canonical_block_prunes_when_child_keeps_cumulative_wire_parent() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0xa1);
        let second_block_tx = create_deploy_log_tx_with_gas_limit(0xa2, 110_000);
        seed_sender_balance(&client, &first_block_tx);
        seed_sender_balance(&client, &second_block_tx);

        let mut engine = HotEngine::new(client.clone(), 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0xa1; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), first_block_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first block should apply");

        let second_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0xa2; 8]),
            active_pending_block_wire_hash(&engine),
            true,
            vec![decoded_l1_info_tx(), second_block_tx],
        );
        engine.apply_flashblock(&second_flashblock).expect("second block should apply");

        let canonical_block = canonical_block_from_pending_block(
            engine.window.blocks.front().expect("first retained block should exist"),
        );
        insert_canonical_header(&client, &canonical_block);

        let outcome = engine
            .process_canonical_block(&canonical_block)
            .expect("matching canonical block should prune when the retained child points at the cumulative wire parent");

        assert!(matches!(outcome, HotApplyOutcome::CanonicalWindowChanged));
        assert_eq!(
            engine.window.anchor,
            HotWindowAnchor::new(1, canonical_block.header().hash_slow())
        );
        assert!(engine.window.execution.is_some());
        assert_eq!(engine.window.blocks.len(), 1);
        assert_eq!(engine.window.active_block_number(), Some(2));
    }

    #[test]
    fn hot_engine_canonical_conflict_resets_without_retained_audit_window() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0xb1);
        let second_block_tx = create_deploy_log_tx_with_gas_limit(0xb2, 110_000);
        seed_sender_balance(&client, &first_block_tx);
        seed_sender_balance(&client, &second_block_tx);

        let mut engine = HotEngine::new(client, 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0xb1; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), first_block_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first block should apply");

        let second_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0xb2; 8]),
            active_pending_block_wire_hash(&engine),
            true,
            vec![decoded_l1_info_tx(), second_block_tx],
        );
        engine.apply_flashblock(&second_flashblock).expect("second block should apply");

        let outcome = engine
            .process_canonical_block(&canonical_block_with_header(
                Header {
                    number: 1,
                    parent_hash: canonical_parent_hash,
                    extra_data: Bytes::from_static(b"canonical-conflict"),
                    ..Default::default()
                },
                vec![],
            ))
            .expect("canonical conflict should reset when no retained audit window exists");

        assert!(matches!(outcome, HotApplyOutcome::Reset));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_canonical_parent_conflict_below_pending_window_resets_without_retained_audit_window()
     {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0xb3);
        let second_block_tx = create_deploy_log_tx_with_gas_limit(0xb4, 110_000);
        seed_sender_balance(&client, &first_block_tx);
        seed_sender_balance(&client, &second_block_tx);

        let mut engine = HotEngine::new(client, 3);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0xb3; 8]),
                canonical_parent_hash,
                true,
                vec![decoded_l1_info_tx(), first_block_tx],
            ))
            .expect("first block should apply");
        let second_parent_hash = active_pending_block_wire_hash(&engine);
        engine
            .apply_flashblock(&flashblock(
                0,
                2,
                PayloadId::new([0xb4; 8]),
                second_parent_hash,
                true,
                vec![decoded_l1_info_tx(), second_block_tx],
            ))
            .expect("second block should apply");

        let outcome = engine
            .process_canonical_block(&canonical_block_with_header(
                Header {
                    number: 0,
                    extra_data: Bytes::from_static(b"canonical-parent-conflict"),
                    ..Default::default()
                },
                vec![],
            ))
            .expect(
                "canonical parent conflict below pending window should reset without a retained audit window",
            );

        assert!(matches!(outcome, HotApplyOutcome::Reset));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_speculative_depth_invalidate_session_fail_closed_without_retained_audit_window() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0xc1);
        let second_block_tx = create_deploy_log_tx_with_gas_limit(0xc2, 110_000);
        let third_block_tx = create_deploy_log_tx_with_gas_limit(0xc3, 120_000);
        seed_sender_balance(&client, &first_block_tx);
        seed_sender_balance(&client, &second_block_tx);
        seed_sender_balance(&client, &third_block_tx);

        let mut engine = HotEngine::new(client, 2);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0xc1; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), first_block_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first block should apply");

        let second_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0xc2; 8]),
            active_pending_block_wire_hash(&engine),
            true,
            vec![decoded_l1_info_tx(), second_block_tx],
        );
        engine.apply_flashblock(&second_flashblock).expect("second block should apply");

        let third_parent_hash = active_pending_block_wire_hash(&engine);
        let outcome = engine
            .apply_flashblock(&flashblock(
                0,
                3,
                PayloadId::new([0xc3; 8]),
                third_parent_hash,
                true,
                vec![decoded_l1_info_tx(), third_block_tx],
            ))
            .expect("depth breach should return a hard invalidation");

        assert!(matches!(
            outcome,
            HotApplyOutcome::InvalidateSession {
                reason: HotInvalidationReason::SpeculativeDepthExceeded
            }
        ));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_speculative_depth_fail_closed_even_with_canonical_anchor() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0xd1);
        let second_block_tx = create_deploy_log_tx_with_gas_limit(0xd2, 110_000);
        let third_block_tx = create_deploy_log_tx_with_gas_limit(0xd3, 120_000);
        let fourth_block_tx = create_deploy_log_tx_with_gas_limit(0xd4, 130_000);
        seed_sender_balance(&client, &first_block_tx);
        seed_sender_balance(&client, &second_block_tx);
        seed_sender_balance(&client, &third_block_tx);
        seed_sender_balance(&client, &fourth_block_tx);

        let mut engine = HotEngine::new(client.clone(), 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0xd1; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), first_block_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first block should apply");

        let second_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0xd2; 8]),
            active_pending_block_wire_hash(&engine),
            true,
            vec![decoded_l1_info_tx(), second_block_tx],
        );
        engine.apply_flashblock(&second_flashblock).expect("second block should apply");

        let third_flashblock = flashblock(
            0,
            3,
            PayloadId::new([0xd3; 8]),
            active_pending_block_wire_hash(&engine),
            true,
            vec![decoded_l1_info_tx(), third_block_tx],
        );
        engine.apply_flashblock(&third_flashblock).expect("third block should apply");

        let canonical_block = canonical_block_from_pending_block(
            engine.window.blocks.front().expect("first retained block should exist"),
        );
        insert_canonical_header(&client, &canonical_block);
        let fourth_parent_hash = active_pending_block_wire_hash(&engine);
        let outcome = engine
            .apply_flashblock(&flashblock(
                0,
                4,
                PayloadId::new([0xd4; 8]),
                fourth_parent_hash,
                true,
                vec![decoded_l1_info_tx(), fourth_block_tx],
            ))
            .expect("depth breach should fail closed in Task 1");

        assert!(matches!(
            outcome,
            HotApplyOutcome::InvalidateSession {
                reason: HotInvalidationReason::SpeculativeDepthExceeded
            }
        ));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_speculative_depth_fail_closed_before_periodic_audit_shadow_rebuild() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0xd5);
        let second_block_tx = create_deploy_log_tx_with_gas_limit(0xd6, 110_000);
        let third_block_tx = create_deploy_log_tx_with_gas_limit(0xd7, 120_000);
        let fourth_block_tx = create_deploy_log_tx_with_gas_limit(0xd8, 130_000);
        seed_sender_balance(&client, &first_block_tx);
        seed_sender_balance(&client, &second_block_tx);
        seed_sender_balance(&client, &third_block_tx);
        seed_sender_balance(&client, &fourth_block_tx);

        let mut engine = HotEngine::new(client.clone(), 3);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0xd5; 8]),
                canonical_parent_hash,
                true,
                vec![decoded_l1_info_tx(), first_block_tx],
            ))
            .expect("first block should apply");
        let second_parent_hash = active_pending_block_wire_hash(&engine);
        engine
            .apply_flashblock(&flashblock(
                0,
                2,
                PayloadId::new([0xd6; 8]),
                second_parent_hash,
                true,
                vec![decoded_l1_info_tx(), second_block_tx],
            ))
            .expect("second block should apply");
        let third_parent_hash = active_pending_block_wire_hash(&engine);
        engine
            .apply_flashblock(&flashblock(
                0,
                3,
                PayloadId::new([0xd7; 8]),
                third_parent_hash,
                true,
                vec![decoded_l1_info_tx(), third_block_tx],
            ))
            .expect("third block should apply");

        let canonical_block = canonical_block_from_pending_block(
            engine.window.blocks.front().expect("first retained block should exist"),
        );
        insert_canonical_header(&client, &canonical_block);

        let fourth_parent_hash = active_pending_block_wire_hash(&engine);
        let outcome = engine
            .apply_flashblock(&flashblock(
                0,
                4,
                PayloadId::new([0xd8; 8]),
                fourth_parent_hash,
                true,
                vec![decoded_l1_info_tx(), fourth_block_tx],
            ))
            .expect("depth breach should fail closed before periodic audit or shadow rebuild");

        assert!(matches!(
            outcome,
            HotApplyOutcome::InvalidateSession {
                reason: HotInvalidationReason::SpeculativeDepthExceeded
            }
        ));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_speculative_depth_fail_closed_before_anchor_canonical_update() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0xe1);
        let second_block_tx = create_deploy_log_tx_with_gas_limit(0xe2, 110_000);
        let third_block_tx = create_deploy_log_tx_with_gas_limit(0xe3, 120_000);
        let fourth_block_tx = create_deploy_log_tx_with_gas_limit(0xe4, 130_000);
        seed_sender_balance(&client, &first_block_tx);
        seed_sender_balance(&client, &second_block_tx);
        seed_sender_balance(&client, &third_block_tx);
        seed_sender_balance(&client, &fourth_block_tx);

        let mut engine = HotEngine::new(client.clone(), 3);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0xe1; 8]),
                canonical_parent_hash,
                true,
                vec![decoded_l1_info_tx(), first_block_tx],
            ))
            .expect("first block should apply");
        let second_parent_hash = active_pending_block_wire_hash(&engine);
        engine
            .apply_flashblock(&flashblock(
                0,
                2,
                PayloadId::new([0xe2; 8]),
                second_parent_hash,
                true,
                vec![decoded_l1_info_tx(), second_block_tx],
            ))
            .expect("second block should apply");
        let third_parent_hash = active_pending_block_wire_hash(&engine);
        engine
            .apply_flashblock(&flashblock(
                0,
                3,
                PayloadId::new([0xe3; 8]),
                third_parent_hash,
                true,
                vec![decoded_l1_info_tx(), third_block_tx],
            ))
            .expect("third block should apply");

        let canonical_anchor_block = canonical_block_from_pending_block(
            engine.window.blocks.front().expect("first retained block should exist"),
        );
        insert_canonical_header(&client, &canonical_anchor_block);

        let fourth_parent_hash = active_pending_block_wire_hash(&engine);
        let rollover_outcome = engine
            .apply_flashblock(&flashblock(
                0,
                4,
                PayloadId::new([0xe4; 8]),
                fourth_parent_hash,
                true,
                vec![decoded_l1_info_tx(), fourth_block_tx],
            ))
            .expect("depth breach should fail closed before any periodic audit or shadow rebuild fallback");

        assert!(matches!(
            rollover_outcome,
            HotApplyOutcome::InvalidateSession {
                reason: HotInvalidationReason::SpeculativeDepthExceeded
            }
        ));
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());

        let outcome = engine
            .process_canonical_block(&canonical_anchor_block)
            .expect("canonical update after fail-closed depth breach should be ignored");

        assert!(matches!(outcome, HotApplyOutcome::Duplicate));
    }

    #[test]
    fn hot_engine_shadow_rebuild_completion_reports_swapped_when_live_window_matches_snapshot() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0xd1);
        seed_sender_balance(&client, &first_block_tx);

        let mut engine = HotEngine::new(client.clone(), 3);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0xd1; 8]),
                canonical_parent_hash,
                true,
                vec![decoded_l1_info_tx(), first_block_tx],
            ))
            .expect("first block should apply");

        let snapshot = engine
            .window
            .make_audit_snapshot(7, 9, 0, canonical_parent_hash)
            .expect("live retained prefix should snapshot");
        let (audit_result, rebuilt_engine) =
            HotEngine::rebuild_shadow_from_snapshot(client, 3, &snapshot);

        engine.next_snapshot_nonce = 99;
        if let Some(active_block) = engine.window.active_block_mut() {
            active_block.transactions.clear();
            active_block.logs.clear();
        }

        let completion =
            engine.complete_shadow_rebuild(7, 9, &snapshot, audit_result, rebuilt_engine);

        assert_eq!(completion, ShadowRebuildCompletion::EquivalentSwapped);
        assert_eq!(engine.next_snapshot_nonce, 99);
        assert_eq!(engine.window.latest_audit_cursor(), Some(snapshot.cursor));
        assert_eq!(engine.window.blocks.len(), 1);
        assert!(
            engine
                .window
                .active_block()
                .expect("rebuilt live block should exist")
                .transactions
                .len()
                >= 2
        );
    }

    #[test]
    fn hot_engine_shadow_rebuild_completion_reports_no_op_when_live_advanced_to_later_block() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0xd2);
        let second_block_tx = create_deploy_log_tx_with_gas_limit(0xd3, 120_000);
        seed_sender_balance(&client, &first_block_tx);
        seed_sender_balance(&client, &second_block_tx);

        let mut engine = HotEngine::new(client.clone(), 3);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0xd2; 8]),
                canonical_parent_hash,
                true,
                vec![decoded_l1_info_tx(), first_block_tx],
            ))
            .expect("first block should apply");

        let snapshot = engine
            .window
            .make_audit_snapshot(7, 9, 0, canonical_parent_hash)
            .expect("first retained block should snapshot");
        let second_parent_hash = active_pending_block_wire_hash(&engine);

        engine
            .apply_flashblock(&flashblock(
                0,
                2,
                PayloadId::new([0xd3; 8]),
                second_parent_hash,
                true,
                vec![decoded_l1_info_tx(), second_block_tx],
            ))
            .expect("second block should advance live window");

        let live_cursor_before_completion =
            engine.window.latest_audit_cursor().expect("live cursor should exist");
        let (audit_result, rebuilt_engine) =
            HotEngine::rebuild_shadow_from_snapshot(client, 3, &snapshot);
        let completion =
            engine.complete_shadow_rebuild(7, 9, &snapshot, audit_result, rebuilt_engine);

        assert_eq!(completion, ShadowRebuildCompletion::EquivalentNoOp);
        assert_eq!(engine.window.anchor.block_number(), 0);
        assert_eq!(engine.window.anchor.hash(), canonical_parent_hash);
        assert_eq!(engine.window.blocks.len(), 2);
        assert_eq!(engine.window.blocks.front().map(|block| block.block_number), Some(1));
        assert_eq!(engine.window.blocks.back().map(|block| block.block_number), Some(2));
        assert_eq!(engine.window.latest_audit_cursor(), Some(live_cursor_before_completion));
    }

    #[test]
    fn hot_engine_shadow_rebuild_equivalent_prefix_is_no_op_when_live_advanced_within_same_block() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let deploy_tx = create_deploy_log_tx(0xd4);
        let contract_address =
            deploy_tx.recover_signer().expect("deploy signer should recover").create(0);
        let call_tx = create_call_log_tx(contract_address, 0xd5);
        seed_sender_balance(&client, &deploy_tx);
        seed_sender_balance(&client, &call_tx);

        let mut engine = HotEngine::new(client.clone(), 3);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0xd4; 8]),
                canonical_parent_hash,
                true,
                vec![decoded_l1_info_tx(), deploy_tx],
            ))
            .expect("first flashblock should apply");

        let snapshot = engine
            .window
            .make_audit_snapshot(7, 9, 0, canonical_parent_hash)
            .expect("partial retained block should snapshot");

        engine
            .apply_flashblock(&flashblock(
                1,
                1,
                PayloadId::new([0xd4; 8]),
                canonical_parent_hash,
                false,
                vec![call_tx],
            ))
            .expect("same-block suffix should advance live window");

        let live_cursor_before_completion =
            engine.window.latest_audit_cursor().expect("live cursor should exist");
        let (audit_result, rebuilt_engine) =
            HotEngine::rebuild_shadow_from_snapshot(client, 3, &snapshot);
        let completion =
            engine.complete_shadow_rebuild(7, 9, &snapshot, audit_result, rebuilt_engine);

        assert_eq!(completion, ShadowRebuildCompletion::EquivalentNoOp);
        assert_eq!(engine.window.anchor.block_number(), 0);
        assert_eq!(engine.window.anchor.hash(), canonical_parent_hash);
        assert_eq!(engine.window.blocks.len(), 1);
        assert_eq!(engine.window.latest_audit_cursor(), Some(live_cursor_before_completion));
        assert_eq!(
            engine
                .window
                .active_block()
                .expect("live block should remain advanced")
                .latest_flashblock_index,
            1
        );
        assert_eq!(
            engine
                .window
                .active_block()
                .expect("live block should remain advanced")
                .published_outputs
                .len(),
            2
        );
    }

    #[test]
    fn hot_engine_shadow_rebuild_anchor_mismatch_fails_instead_of_swapping() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0xd6);
        seed_sender_balance(&client, &first_block_tx);

        let mut engine = HotEngine::new(client.clone(), 3);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0xd6; 8]),
                canonical_parent_hash,
                true,
                vec![decoded_l1_info_tx(), first_block_tx],
            ))
            .expect("first block should apply");

        let snapshot = engine
            .window
            .make_audit_snapshot(7, 9, 0, canonical_parent_hash)
            .expect("live retained prefix should snapshot");
        let (audit_result, rebuilt_engine) =
            HotEngine::rebuild_shadow_from_snapshot(client, 3, &snapshot);

        engine.window.anchor = HotWindowAnchor::new(0, B256::with_last_byte(0xee));

        let completion =
            engine.complete_shadow_rebuild(7, 9, &snapshot, audit_result, rebuilt_engine);

        assert_eq!(
            completion,
            ShadowRebuildCompletion::ActiveFailed { failure: PeriodicAuditFailure::AnchorMismatch }
        );
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_shadow_rebuild_same_cursor_with_different_prefix_fails_instead_of_swapping() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0xd6);
        seed_sender_balance(&client, &first_block_tx);

        let mut engine = HotEngine::new(client.clone(), 3);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0xd4; 8]),
                canonical_parent_hash,
                true,
                vec![decoded_l1_info_tx(), first_block_tx],
            ))
            .expect("first block should apply");

        let snapshot = engine
            .window
            .make_audit_snapshot(7, 9, 0, canonical_parent_hash)
            .expect("live retained prefix should snapshot");
        let (audit_result, rebuilt_engine) =
            HotEngine::rebuild_shadow_from_snapshot(client, 3, &snapshot);

        engine.window.active_block_mut().expect("active block should exist").published_outputs[0]
            .block_timestamp = Some(1_234_567_890);

        let completion =
            engine.complete_shadow_rebuild(7, 9, &snapshot, audit_result, rebuilt_engine);

        assert_eq!(
            completion,
            ShadowRebuildCompletion::ActiveFailed { failure: PeriodicAuditFailure::Mismatch }
        );
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }

    #[test]
    fn hot_engine_live_flashblock_prefix_mismatch_is_classified() {
        let canonical_parent_hash = B256::with_last_byte(0x44);
        let expected_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0xa1; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx()],
        );
        let live_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0xa2; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx()],
        );

        let mismatch = HotEngine::<MockEthProvider<BasePrimitives, Arc<BaseChainSpec>>>::
            flashblock_prefix_mismatch(
                std::slice::from_ref(&expected_flashblock),
                Some(std::slice::from_ref(&live_flashblock)),
            );

        assert_eq!(
            mismatch,
            Some(FlashblockPrefixMismatch {
                live_len: Some(1),
                mismatch_index: Some(0),
                expected_block_number: Some(1),
                live_block_number: Some(1),
                expected_flashblock_index: Some(0),
                live_flashblock_index: Some(0),
                expected_payload_id: Some(expected_flashblock.payload_id),
                live_payload_id: Some(live_flashblock.payload_id),
                expected_parent_hash: Some(canonical_parent_hash),
                live_parent_hash: Some(canonical_parent_hash),
            })
        );
    }

    #[test]
    fn hot_engine_live_output_prefix_mismatch_is_classified() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0xd8);
        seed_sender_balance(&client, &first_block_tx);

        let mut engine = HotEngine::new(client, 3);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0xd8; 8]),
                canonical_parent_hash,
                true,
                vec![decoded_l1_info_tx(), first_block_tx],
            ))
            .expect("first block should apply");

        let snapshot = engine
            .window
            .make_audit_snapshot(7, 9, 0, canonical_parent_hash)
            .expect("live retained prefix should snapshot");
        let expected_output = snapshot.expected_outputs[0].clone();
        let mut live_output = expected_output.clone();
        live_output.logs[0].log_index_in_block =
            live_output.logs[0].log_index_in_block.saturating_add(1);

        let mismatch = HotEngine::<MockEthProvider<BasePrimitives, Arc<BaseChainSpec>>>::
            retained_output_prefix_mismatch(
                std::slice::from_ref(&expected_output),
                Some(std::slice::from_ref(&live_output)),
            );

        assert_eq!(
            mismatch,
            Some(RetainedOutputPrefixMismatch {
                live_len: Some(1),
                mismatch_index: Some(0),
                mismatch: Some(PeriodicAuditOutputMismatch {
                    index: 0,
                    field: PeriodicAuditOutputMismatchField::Logs,
                    expected_cursor: expected_output.cursor,
                    actual_cursor: live_output.cursor,
                }),
            })
        );
    }

    #[test]
    fn hot_engine_shadow_rebuild_stale_window_id_is_ignored() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0xd7);
        seed_sender_balance(&client, &first_block_tx);

        let mut engine = HotEngine::new(client, 3);
        engine
            .apply_flashblock(&flashblock(
                0,
                1,
                PayloadId::new([0xd7; 8]),
                canonical_parent_hash,
                true,
                vec![decoded_l1_info_tx(), first_block_tx],
            ))
            .expect("first block should apply");

        let snapshot = engine
            .window
            .make_audit_snapshot(7, 9, 0, canonical_parent_hash)
            .expect("live retained prefix should snapshot");
        let live_cursor = engine.window.latest_audit_cursor();

        let completion = engine.complete_shadow_rebuild(
            7,
            10,
            &snapshot,
            PeriodicAuditResult::Diverged { failure: PeriodicAuditFailure::Mismatch },
            None,
        );

        assert_eq!(completion, ShadowRebuildCompletion::StaleIgnored);
        assert!(engine.window.execution.is_some());
        assert_eq!(engine.window.blocks.len(), 1);
        assert_eq!(engine.window.latest_audit_cursor(), live_cursor);
    }

    #[test]
    fn hot_engine_reset_clears_speculative_state_without_retained_audit_window() {
        let client = test_client();
        let canonical_parent_hash = client.chain_spec().genesis_hash();
        let first_block_tx = create_deploy_log_tx(0x81);
        let second_block_tx = create_deploy_log_tx_with_gas_limit(0x82, 110_000);
        seed_sender_balance(&client, &first_block_tx);
        seed_sender_balance(&client, &second_block_tx);

        let mut engine = HotEngine::new(client, 3);
        let first_flashblock = flashblock(
            0,
            1,
            PayloadId::new([0x71; 8]),
            canonical_parent_hash,
            true,
            vec![decoded_l1_info_tx(), first_block_tx],
        );
        engine.apply_flashblock(&first_flashblock).expect("first block should apply");

        let second_parent_hash = active_pending_block_wire_hash(&engine);
        let second_flashblock = flashblock(
            0,
            2,
            PayloadId::new([0x72; 8]),
            second_parent_hash,
            true,
            vec![decoded_l1_info_tx(), second_block_tx],
        );
        engine.apply_flashblock(&second_flashblock).expect("second block should apply");

        assert_eq!(engine.window.anchor.block_number(), 0);
        assert_eq!(engine.window.blocks.len(), 2);

        engine.reset();

        assert_eq!(engine.window.anchor.block_number(), 0);
        assert!(engine.window.execution.is_none());
        assert!(engine.window.blocks.is_empty());
    }
}
