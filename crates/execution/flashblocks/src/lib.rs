#![doc = include_str!("../README.md")]
#![doc(
    html_logo_url = "https://avatars.githubusercontent.com/u/16627100?s=200&v=4",
    html_favicon_url = "https://avatars.githubusercontent.com/u/16627100?s=200&v=4",
    issue_tracker_base_url = "https://github.com/base/base/issues/"
)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

#[macro_use]
extern crate tracing;

mod block_assembler;
pub use block_assembler::{AssembledBlock, BlockAssembler};

mod cache;
pub use cache::{CachedFlashblock, FlashblockCache};

mod error;
pub use error::{
    BuildError, ExecutionError, ProtocolError, ProviderError, Result, StateProcessorError,
};

mod fast_logs;
pub use fast_logs::{
    FastFlashblockFeedEvent, FastFlashblockLog, FastFlashblockLogsDelta,
    FastFlashblockLogsDeltaError, FastFlashblockTxMeta, FlashblockSnapshotId,
};

mod hot_engine;
pub use hot_engine::{
    HotApplyOutcome, HotEngine, HotExecutionDb, HotInvalidationReason, ShadowRebuildCompletion,
};

mod hot_overlay;
pub use hot_overlay::{HotOverlay, HotOverlayDb, HotOverlayError, OverlayAccount};

mod hot_mode;
pub use hot_mode::FlashblocksMode;

mod hot_snapshot;
pub use hot_snapshot::{HotSnapshot, HotSnapshotRing};

mod hot_window;
pub use hot_window::{
    HotExecutedHeaderParts, HotExecutionState, HotPendingBlock, HotPendingWindow, HotWindowAnchor,
};

mod periodic_audit;
pub use periodic_audit::{
    AuditCursor, AuditWindowSnapshot, PeriodicAuditFailure, PeriodicAuditResult, RetainedFastOutput,
};

mod metrics;
pub use metrics::Metrics;

mod pending_blocks;
pub use pending_blocks::{PendingBlocks, PendingBlocksBuilder};

mod snapshot_cache;
pub use snapshot_cache::SnapshotCache;

mod processor;
pub use processor::{StateProcessor, StateProcessorHandles, StateUpdate};

mod state;
pub use state::FlashblocksState;

mod subscription;
pub use subscription::FlashblocksSubscriber;

mod traits;
pub use traits::{FlashblocksAPI, FlashblocksReceiver, PendingBlocksAPI};

mod state_builder;
pub use state_builder::{ExecutedPendingTransaction, PendingHeaderBuilder, PendingStateBuilder};

mod receipt_builder;
pub use receipt_builder::{ReceiptBuildError, UnifiedReceiptBuilder};

mod validation;
pub use validation::{
    CanonicalBlockReconciler, FlashblockSequenceValidator, ReconciliationStrategy,
    ReorgDetectionResult, ReorgDetector, SequenceValidationResult,
};

mod config;
pub use config::FlashblocksConfig;

mod rpc;
pub use rpc::FlashblockDryRunResult;
pub use rpc::{
    BaseSubscriptionKind, BlockNumberOrTagExt, EthApiExt, EthApiOverrideServer, EthPubSub,
    EthPubSubApiServer, ExtendedSubscriptionKind, FlashblockLog, FlashblockLogsBatch,
    FlashblockTxMeta, TransactionWithLogs,
};
