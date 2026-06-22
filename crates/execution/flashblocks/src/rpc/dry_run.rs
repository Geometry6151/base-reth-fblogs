//! Dry-run RPC result types for flashblocks.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use alloy_evm::{env::BlockEnvironment, overrides::apply_block_overrides};
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_rpc_types::state::EvmOverrides;
use base_common_network::Base;
use base_common_rpc_types::BaseTransactionRequest;
use jsonrpsee::core::RpcResult;
use jsonrpsee_types::{ErrorObjectOwned, error::INTERNAL_ERROR_CODE};
use reth_revm::{database::StateProviderDatabase, db::State};
use reth_rpc_eth_api::{FromEthApiError, helpers::FullEthApi};
use reth_rpc_eth_types::{EthApiError, cache::db::StateProviderTraitObjWrapper};
use revm::{
    Database,
    context::result::ExecutionResult,
    state::{AccountInfo, Bytecode},
};
use serde::{Deserialize, Serialize};

use crate::{FlashblockSnapshotId, HotDryRunSeed, HotOverlay, HotOverlayDb, HotSnapshot, Metrics};

/// Result returned by hot flashblock dry-run RPC methods.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlashblockDryRunResult {
    /// True when the dry-run EVM execution returned a successful result.
    pub success: bool,
    /// Raw revert bytes for EVM reverts.
    pub revert: Option<Bytes>,
    /// Halt reason for non-revert EVM halts.
    pub halt: Option<String>,
    /// Gas used by the single dry-run execution.
    #[serde(with = "alloy_serde::quantity")]
    pub gas_used: u64,
    /// Snapshot actually used by the dry run.
    pub snapshot_id: FlashblockSnapshotId,
}

impl FlashblockDryRunResult {
    /// Constructs a successful dry-run result.
    pub const fn success(snapshot_id: FlashblockSnapshotId, gas_used: u64) -> Self {
        Self { success: true, revert: None, halt: None, gas_used, snapshot_id }
    }

    /// Constructs a reverting dry-run result.
    pub fn revert(snapshot_id: FlashblockSnapshotId, revert: Bytes, gas_used: u64) -> Self {
        Self { success: false, revert: Some(revert), halt: None, gas_used, snapshot_id }
    }

    /// Constructs a halted dry-run result.
    pub fn halt(snapshot_id: FlashblockSnapshotId, halt: String, gas_used: u64) -> Self {
        Self { success: false, revert: None, halt: Some(halt), gas_used, snapshot_id }
    }
}

#[derive(Debug, Default)]
struct CanonicalReadCounts {
    account_reads: AtomicU64,
    storage_reads: AtomicU64,
    code_reads: AtomicU64,
    block_hash_reads: AtomicU64,
}

impl CanonicalReadCounts {
    fn increment_account_reads(&self) {
        self.account_reads.fetch_add(1, Ordering::Relaxed);
    }

    fn increment_storage_reads(&self) {
        self.storage_reads.fetch_add(1, Ordering::Relaxed);
    }

    fn increment_code_reads(&self) {
        self.code_reads.fetch_add(1, Ordering::Relaxed);
    }

    fn increment_block_hash_reads(&self) {
        self.block_hash_reads.fetch_add(1, Ordering::Relaxed);
    }

    fn account_reads(&self) -> u64 {
        self.account_reads.load(Ordering::Relaxed)
    }

    fn storage_reads(&self) -> u64 {
        self.storage_reads.load(Ordering::Relaxed)
    }

    fn code_reads(&self) -> u64 {
        self.code_reads.load(Ordering::Relaxed)
    }

    fn block_hash_reads(&self) -> u64 {
        self.block_hash_reads.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
struct CountingCanonicalDb<DB> {
    inner: DB,
    read_counts: Arc<CanonicalReadCounts>,
}

impl<DB> CountingCanonicalDb<DB> {
    fn new(inner: DB, read_counts: Arc<CanonicalReadCounts>) -> Self {
        Self { inner, read_counts }
    }
}

impl<DB> Database for CountingCanonicalDb<DB>
where
    DB: Database,
{
    type Error = DB::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.read_counts.increment_account_reads();
        self.inner.basic(address)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.read_counts.increment_code_reads();
        self.inner.code_by_hash(code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        self.read_counts.increment_storage_reads();
        self.inner.storage(address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.read_counts.increment_block_hash_reads();
        self.inner.block_hash(number)
    }
}

fn record_total_duration(method: &'static str, duration: Duration) {
    match method {
        "latest" => Metrics::rpc_base_dry_run_latest_duration().record(duration),
        "at" => Metrics::rpc_base_dry_run_at_duration().record(duration),
        _ => unreachable!("validated method tag"),
    }
}

fn record_latest_canonical_read_counts(read_counts: &CanonicalReadCounts) {
    Metrics::rpc_base_dry_run_latest_account_reads().record(read_counts.account_reads() as f64);
    Metrics::rpc_base_dry_run_latest_storage_reads().record(read_counts.storage_reads() as f64);
    Metrics::rpc_base_dry_run_latest_code_reads().record(read_counts.code_reads() as f64);
    Metrics::rpc_base_dry_run_latest_block_hash_reads()
        .record(read_counts.block_hash_reads() as f64);
}

fn increment_outcome_metric(result: &FlashblockDryRunResult) -> &'static str {
    if result.success {
        Metrics::rpc_base_dry_run_success_count().increment(1);
        "success"
    } else if result.revert.is_some() {
        Metrics::rpc_base_dry_run_revert_count().increment(1);
        "revert"
    } else {
        Metrics::rpc_base_dry_run_halt_count().increment(1);
        "halt"
    }
}

fn latest_seed_matches_snapshot(
    snapshot_id: FlashblockSnapshotId,
    seed_snapshot_id: Option<FlashblockSnapshotId>,
) -> bool {
    seed_snapshot_id == Some(snapshot_id)
}

pub(crate) async fn dry_run_hot_snapshot<Eth>(
    eth_api: &Eth,
    method: &'static str,
    snapshot: Arc<HotSnapshot>,
    transaction: BaseTransactionRequest,
) -> RpcResult<FlashblockDryRunResult>
where
    Eth: FullEthApi<NetworkTypes = Base> + Send + Sync + 'static,
    ErrorObjectOwned: From<Eth::Error>,
{
    dry_run_hot_snapshot_inner(eth_api, method, snapshot, None, transaction).await
}

pub(crate) async fn dry_run_latest_hot_snapshot<Eth>(
    eth_api: &Eth,
    snapshot: Arc<HotSnapshot>,
    latest_seed: Option<Arc<HotDryRunSeed>>,
    transaction: BaseTransactionRequest,
) -> RpcResult<FlashblockDryRunResult>
where
    Eth: FullEthApi<NetworkTypes = Base> + Send + Sync + 'static,
    ErrorObjectOwned: From<Eth::Error>,
{
    let latest_start = Instant::now();
    let (seed, used_seed) = match latest_seed {
        Some(seed)
            if latest_seed_matches_snapshot(snapshot.snapshot_id, Some(seed.snapshot_id)) =>
        {
            (Some(seed), true)
        }
        Some(_) => {
            Metrics::rpc_base_dry_run_latest_seed_stale_count().increment(1);
            Metrics::rpc_base_dry_run_latest_direct_fallback_count().increment(1);
            (None, false)
        }
        None => {
            Metrics::rpc_base_dry_run_latest_direct_fallback_count().increment(1);
            (None, false)
        }
    };

    let result = dry_run_hot_snapshot_inner(eth_api, "latest", snapshot, seed, transaction).await;
    if used_seed {
        Metrics::rpc_base_dry_run_latest_seed_hit_count().increment(1);
        Metrics::rpc_base_dry_run_latest_seed_hit_duration().record(latest_start.elapsed());
    }

    result
}

async fn dry_run_hot_snapshot_inner<Eth>(
    eth_api: &Eth,
    method: &'static str,
    snapshot: Arc<HotSnapshot>,
    seed: Option<Arc<HotDryRunSeed>>,
    transaction: BaseTransactionRequest,
) -> RpcResult<FlashblockDryRunResult>
where
    Eth: FullEthApi<NetworkTypes = Base> + Send + Sync + 'static,
    ErrorObjectOwned: From<Eth::Error>,
{
    debug_assert!(matches!(method, "latest" | "at"));

    let total_start = Instant::now();
    let snapshot_id = snapshot.snapshot_id;
    let canonical_base_block = snapshot.canonical_base_block;
    let block_overrides = snapshot.block_overrides.clone();
    let permit = match eth_api.acquire_owned_blocking_io().await {
        Ok(permit) => permit,
        Err(error) => {
            Metrics::rpc_base_dry_run_error_count().increment(1);
            record_total_duration(method, total_start.elapsed());
            return Err(ErrorObjectOwned::owned(
                INTERNAL_ERROR_CODE,
                error.to_string(),
                None::<()>,
            ));
        }
    };
    let _permit = permit;

    let overlay_start = Instant::now();
    let overlay = snapshot
        .dry_run_overlay()
        .get_or_init(|| HotOverlay::from_state_override(&snapshot.state_overrides).map(Arc::new))
        .as_ref();
    Metrics::rpc_base_dry_run_overlay_init_duration().record(overlay_start.elapsed());
    let overlay = overlay
        .map(Arc::clone)
        .map_err(|err| Eth::Error::from_eth_err(EthApiError::InvalidParams(err.to_string())))?;
    let overlay_accounts = overlay.account_count();
    let overlay_slots = overlay.storage_slot_count();
    Metrics::rpc_base_dry_run_overlay_account_count().record(overlay_accounts as f64);
    Metrics::rpc_base_dry_run_overlay_slot_count().record(overlay_slots as f64);

    let canonical_env_start = Instant::now();
    let (mut evm_env, at) = eth_api.evm_env_at(canonical_base_block).await?;
    Metrics::rpc_base_dry_run_canonical_env_duration().record(canonical_env_start.elapsed());

    let state_open_start = Instant::now();
    let state = eth_api.state_at_block_id(at).await?;
    Metrics::rpc_base_dry_run_canonical_state_open_duration().record(state_open_start.elapsed());

    let read_counts = Arc::new(CanonicalReadCounts::default());
    let dry_run_read_counts = Arc::clone(&read_counts);

    let dry_run = eth_api
        .spawn_blocking_io(move |this| {
            let canonical = CountingCanonicalDb::new(
                StateProviderDatabase::new(StateProviderTraitObjWrapper(state)),
                dry_run_read_counts,
            );
            let overlay_db = HotOverlayDb::new(canonical, overlay);
            let mut db = match seed {
                Some(seed) => seed.build_request_state(overlay_db),
                None => State::builder().with_database(overlay_db).build(),
            };

            let env_build_start = Instant::now();
            apply_block_overrides(block_overrides, &mut db, evm_env.block_env.inner_mut());

            let prepared_env =
                this.prepare_call_env(evm_env, transaction, &mut db, EvmOverrides::default());
            Metrics::rpc_base_dry_run_env_build_duration().record(env_build_start.elapsed());
            let (evm_env, tx_env) = prepared_env?;

            let evm_start = Instant::now();
            let execution = this.transact(db, evm_env, tx_env);
            let evm_duration = evm_start.elapsed();
            Metrics::rpc_base_dry_run_evm_duration().record(evm_duration);
            let execution = execution?;

            Ok((execution, overlay_accounts, overlay_slots, evm_duration))
        })
        .await;

    let (result, overlay_accounts, overlay_slots, evm_duration) = match dry_run {
        Ok(result) => result,
        Err(error) => {
            Metrics::rpc_base_dry_run_error_count().increment(1);
            record_total_duration(method, total_start.elapsed());
            return Err(error.into());
        }
    };

    if method == "latest" {
        record_latest_canonical_read_counts(read_counts.as_ref());
    }

    let canonical_account_reads = read_counts.account_reads();
    let canonical_storage_reads = read_counts.storage_reads();
    let canonical_code_reads = read_counts.code_reads();
    let canonical_block_hash_reads = read_counts.block_hash_reads();

    let result = match result.result {
        ExecutionResult::Success { gas_used, .. } => {
            FlashblockDryRunResult::success(snapshot_id, gas_used)
        }
        ExecutionResult::Revert { gas_used, output } => {
            FlashblockDryRunResult::revert(snapshot_id, output, gas_used)
        }
        ExecutionResult::Halt { gas_used, reason } => {
            FlashblockDryRunResult::halt(snapshot_id, format!("{reason:?}"), gas_used)
        }
    };

    let outcome = increment_outcome_metric(&result);
    let total_duration = total_start.elapsed();
    record_total_duration(method, total_duration);

    debug!(
        target: "flashblocks::dry_run",
        method,
        gas_used = result.gas_used,
        result = outcome,
        overlay_accounts,
        overlay_slots,
        canonical_account_reads,
        canonical_storage_reads,
        canonical_code_reads,
        canonical_block_hash_reads,
        total_us = total_duration.as_micros(),
        evm_us = evm_duration.as_micros(),
        "completed hot snapshot dry run"
    );

    Ok(result)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{B256, Bytes};
    use alloy_rpc_types_engine::PayloadId;

    use super::{FlashblockDryRunResult, latest_seed_matches_snapshot};
    use crate::FlashblockSnapshotId;

    const EXPECTED_SNAPSHOT_ID_JSON: &str = r#"{"nonce":"0x7","blockNumber":"0x2d62574","flashblockIndex":"0x3","payloadId":"0x1111111111111111","parentHash":"0x2222222222222222222222222222222222222222222222222222222222222222"}"#;

    fn snapshot_id() -> FlashblockSnapshotId {
        FlashblockSnapshotId::new(
            7,
            47_588_724,
            3,
            PayloadId::new([0x11; 8]),
            B256::repeat_byte(0x22),
        )
    }

    fn different_snapshot_id() -> FlashblockSnapshotId {
        FlashblockSnapshotId::new(
            8,
            47_588_725,
            4,
            PayloadId::new([0x12; 8]),
            B256::repeat_byte(0x23),
        )
    }

    #[test]
    fn dry_run_result_serializes_success_with_complete_snapshot_id() {
        let json = serde_json::to_string(&FlashblockDryRunResult::success(snapshot_id(), 543_916))
            .unwrap();

        assert_eq!(
            json,
            format!(
                r#"{{"success":true,"revert":null,"halt":null,"gasUsed":"0x84cac","snapshotId":{}}}"#,
                EXPECTED_SNAPSHOT_ID_JSON,
            )
        );
    }

    #[test]
    fn dry_run_result_serializes_revert_bytes() {
        let json = serde_json::to_string(&FlashblockDryRunResult::revert(
            snapshot_id(),
            Bytes::from_static(&[0xaa, 0xbb]),
            12_345,
        ))
        .unwrap();

        assert_eq!(
            json,
            format!(
                r#"{{"success":false,"revert":"0xaabb","halt":null,"gasUsed":"0x3039","snapshotId":{}}}"#,
                EXPECTED_SNAPSHOT_ID_JSON,
            )
        );
    }

    #[test]
    fn dry_run_result_serializes_halt_string() {
        let json = serde_json::to_string(&FlashblockDryRunResult::halt(
            snapshot_id(),
            "OutOfGas".to_string(),
            99_999,
        ))
        .unwrap();

        assert_eq!(
            json,
            format!(
                r#"{{"success":false,"revert":null,"halt":"OutOfGas","gasUsed":"0x1869f","snapshotId":{}}}"#,
                EXPECTED_SNAPSHOT_ID_JSON,
            )
        );
    }

    #[test]
    fn dry_run_result_serializes_null_fields_when_absent() {
        let json =
            serde_json::to_string(&FlashblockDryRunResult::success(snapshot_id(), 0)).unwrap();

        assert_eq!(
            json,
            format!(
                r#"{{"success":true,"revert":null,"halt":null,"gasUsed":"0x0","snapshotId":{}}}"#,
                EXPECTED_SNAPSHOT_ID_JSON,
            )
        );
    }

    #[test]
    fn latest_seed_matches_snapshot_requires_exact_snapshot_id_match() {
        let snapshot_id = snapshot_id();

        assert!(latest_seed_matches_snapshot(snapshot_id, Some(snapshot_id)));
        assert!(!latest_seed_matches_snapshot(snapshot_id, Some(different_snapshot_id())));
        assert!(!latest_seed_matches_snapshot(snapshot_id, None));
    }
}
