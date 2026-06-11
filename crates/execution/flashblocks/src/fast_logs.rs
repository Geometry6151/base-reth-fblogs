//! Fast log delta types for flashblocks.

use std::{collections::HashSet, sync::Arc};

use alloy_primitives::{Address, B256, Bytes, Log as PrimitiveLog, LogData};
use alloy_rpc_types_engine::PayloadId;
use alloy_rpc_types_eth::Filter;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Snapshot identifier for a flashblock logs state.
///
/// This is a local cursor for the fast-logs stream emitted by this process. The
/// [`nonce`](Self::nonce) is only guaranteed to increase within that in-memory stream and must
/// not be treated as a globally stable sequence number across restarts or different pending block
/// identities.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlashblockSnapshotId {
    /// Opaque monotonic nonce for local snapshot ordering within one fast-logs stream.
    #[serde(with = "alloy_serde::quantity")]
    nonce: u64,
    /// Block number for the pending block being assembled.
    #[serde(with = "alloy_serde::quantity")]
    block_number: u64,
    /// Index of the flashblock within the pending block.
    #[serde(with = "alloy_serde::quantity")]
    flashblock_index: u64,
    /// Engine payload identifier for the pending block.
    payload_id: PayloadId,
    /// Parent hash for the pending block.
    parent_hash: B256,
}

impl FlashblockSnapshotId {
    /// Creates a new snapshot identifier.
    pub const fn new(
        nonce: u64,
        block_number: u64,
        flashblock_index: u64,
        payload_id: PayloadId,
        parent_hash: B256,
    ) -> Self {
        Self { nonce, block_number, flashblock_index, payload_id, parent_hash }
    }

    /// Returns the opaque monotonic snapshot nonce.
    pub const fn nonce(&self) -> u64 {
        self.nonce
    }

    /// Returns the pending block number.
    pub const fn block_number(&self) -> u64 {
        self.block_number
    }

    /// Returns the flashblock index within the pending block.
    pub const fn flashblock_index(&self) -> u64 {
        self.flashblock_index
    }

    /// Returns the payload identifier.
    pub const fn payload_id(&self) -> PayloadId {
        self.payload_id
    }

    /// Returns the parent hash.
    pub const fn parent_hash(&self) -> B256 {
        self.parent_hash
    }
}

/// Internal event for the fast-log feed.
///
/// Current legacy producers emit [`Self::Delta`] only. Hot-only producers may emit [`Self::Resync`]
/// when the local pending window is invalidated, or [`Self::InvalidateSession`] when the stream
/// must terminate and force client reconnect.
#[derive(Clone, Debug)]
pub enum FastFlashblockFeedEvent {
    /// Exact log delta for one flashblock.
    Delta(Arc<FastFlashblockLogsDelta>),
    /// Hot-only reset signaling.
    ///
    /// Consumers that receive this must treat the fast stream as discontinuous until a later
    /// [`Self::Delta`] arrives.
    Resync,
    /// Terminal session invalidation signaling.
    ///
    /// Consumers that receive this must close the derived subscription stream and require the
    /// client to reconnect for a new session.
    InvalidateSession,
}

/// Returned when a [`FastFlashblockLogsDelta`] duplicates identity fields that disagree with its
/// [`FlashblockSnapshotId`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FastFlashblockLogsDeltaError {
    /// The top-level block number differs from [`FastFlashblockLogsDelta::snapshot_id`].
    #[error("snapshot_id.block_number does not match block_number")]
    BlockNumberMismatch,
    /// The top-level flashblock index differs from [`FastFlashblockLogsDelta::snapshot_id`].
    #[error("snapshot_id.flashblock_index does not match flashblock_index")]
    FlashblockIndexMismatch,
    /// The top-level payload identifier differs from [`FastFlashblockLogsDelta::snapshot_id`].
    #[error("snapshot_id.payload_id does not match payload_id")]
    PayloadIdMismatch,
    /// The top-level parent hash differs from [`FastFlashblockLogsDelta::snapshot_id`].
    #[error("snapshot_id.parent_hash does not match parent_hash")]
    ParentHashMismatch,
}

/// Incremental fast logs delta for a flashblock update.
///
/// This hot-path local DTO deliberately repeats the pending block identity at the top level so
/// consumers can read block and payload metadata without unpacking [`snapshot_id`](Self::snapshot_id).
/// Those duplicated fields must always match the embedded [`FlashblockSnapshotId`]; use
/// [`Self::new`] for locally constructed deltas, or Serde round-tripping to validate externally
/// supplied identity fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FastFlashblockLogsDelta {
    /// Snapshot identifier for this delta stream position.
    pub snapshot_id: FlashblockSnapshotId,
    /// Block number for the pending block being assembled.
    ///
    /// Must match [`FlashblockSnapshotId::block_number`] on [`Self::snapshot_id`].
    pub block_number: u64,
    /// Index of this flashblock within the pending block.
    ///
    /// Must match [`FlashblockSnapshotId::flashblock_index`] on [`Self::snapshot_id`].
    pub flashblock_index: u64,
    /// Engine payload identifier for the pending block.
    ///
    /// Must match [`FlashblockSnapshotId::payload_id`] on [`Self::snapshot_id`].
    pub payload_id: PayloadId,
    /// Parent hash for the pending block.
    ///
    /// Must match [`FlashblockSnapshotId::parent_hash`] on [`Self::snapshot_id`].
    pub parent_hash: B256,
    /// Optional timestamp for the pending block.
    pub block_timestamp: Option<u64>,
    /// Logs emitted in this flashblock update.
    pub logs: Vec<FastFlashblockLog>,
    /// Transaction metadata referenced by the logs in this delta.
    pub transactions: Vec<FastFlashblockTxMeta>,
}

impl FastFlashblockLogsDelta {
    /// Creates a fast logs delta by deriving the duplicated identity fields from the supplied
    /// [`FlashblockSnapshotId`].
    pub const fn new(
        snapshot_id: FlashblockSnapshotId,
        block_timestamp: Option<u64>,
        logs: Vec<FastFlashblockLog>,
        transactions: Vec<FastFlashblockTxMeta>,
    ) -> Self {
        Self {
            block_number: snapshot_id.block_number(),
            flashblock_index: snapshot_id.flashblock_index(),
            payload_id: snapshot_id.payload_id(),
            parent_hash: snapshot_id.parent_hash(),
            snapshot_id,
            block_timestamp,
            logs,
            transactions,
        }
    }

    /// Verifies that the duplicated top-level identity fields still match
    /// [`Self::snapshot_id`].
    pub fn validate_identity_fields(&self) -> Result<(), FastFlashblockLogsDeltaError> {
        if self.snapshot_id.block_number() != self.block_number {
            return Err(FastFlashblockLogsDeltaError::BlockNumberMismatch);
        }
        if self.snapshot_id.flashblock_index() != self.flashblock_index {
            return Err(FastFlashblockLogsDeltaError::FlashblockIndexMismatch);
        }
        if self.snapshot_id.payload_id() != self.payload_id {
            return Err(FastFlashblockLogsDeltaError::PayloadIdMismatch);
        }
        if self.snapshot_id.parent_hash() != self.parent_hash {
            return Err(FastFlashblockLogsDeltaError::ParentHashMismatch);
        }
        Ok(())
    }

    /// Returns a filtered copy that preserves block identity while retaining only matching logs
    /// and referenced transaction metadata.
    pub fn filtered(&self, filter: &Filter) -> Self {
        let logs = self
            .logs
            .iter()
            .filter(|log| {
                filter.matches(&PrimitiveLog {
                    address: log.address,
                    data: LogData::new_unchecked(log.topics.clone(), log.data.clone()),
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        let referenced_tx_hashes = logs.iter().map(|log| log.tx_hash).collect::<HashSet<_>>();
        let transactions = self
            .transactions
            .iter()
            .filter(|tx| referenced_tx_hashes.contains(&tx.hash))
            .cloned()
            .collect::<Vec<_>>();

        Self::new(self.snapshot_id, self.block_timestamp, logs, transactions)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FastFlashblockLogsDeltaSerde<'a> {
    snapshot_id: &'a FlashblockSnapshotId,
    #[serde(with = "alloy_serde::quantity")]
    block_number: u64,
    #[serde(with = "alloy_serde::quantity")]
    flashblock_index: u64,
    payload_id: PayloadId,
    parent_hash: B256,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "alloy_serde::quantity::opt")]
    block_timestamp: Option<u64>,
    logs: &'a [FastFlashblockLog],
    transactions: &'a [FastFlashblockTxMeta],
}

impl Serialize for FastFlashblockLogsDelta {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate_identity_fields().map_err(serde::ser::Error::custom)?;

        FastFlashblockLogsDeltaSerde {
            snapshot_id: &self.snapshot_id,
            block_number: self.block_number,
            flashblock_index: self.flashblock_index,
            payload_id: self.payload_id,
            parent_hash: self.parent_hash,
            block_timestamp: self.block_timestamp,
            logs: &self.logs,
            transactions: &self.transactions,
        }
        .serialize(serializer)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FastFlashblockLogsDeltaOwned {
    snapshot_id: FlashblockSnapshotId,
    #[serde(with = "alloy_serde::quantity")]
    block_number: u64,
    #[serde(with = "alloy_serde::quantity")]
    flashblock_index: u64,
    payload_id: PayloadId,
    parent_hash: B256,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "alloy_serde::quantity::opt")]
    block_timestamp: Option<u64>,
    logs: Vec<FastFlashblockLog>,
    transactions: Vec<FastFlashblockTxMeta>,
}

impl<'de> Deserialize<'de> for FastFlashblockLogsDelta {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let owned = FastFlashblockLogsDeltaOwned::deserialize(deserializer)?;
        let delta = Self {
            snapshot_id: owned.snapshot_id,
            block_number: owned.block_number,
            flashblock_index: owned.flashblock_index,
            payload_id: owned.payload_id,
            parent_hash: owned.parent_hash,
            block_timestamp: owned.block_timestamp,
            logs: owned.logs,
            transactions: owned.transactions,
        };
        delta.validate_identity_fields().map_err(serde::de::Error::custom)?;
        Ok(delta)
    }
}

/// Fast log entry included in a [`FastFlashblockLogsDelta`].
///
/// This intentionally differs from [`crate::FlashblockLog`]: it is a hot-path local contract for
/// fast flashblock deltas and omits the `removed` flag carried by the batch-oriented public DTO.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FastFlashblockLog {
    /// Hash of the transaction that emitted the log.
    pub tx_hash: B256,
    /// Transaction index within the pending block.
    #[serde(with = "alloy_serde::quantity")]
    pub tx_index: u64,
    /// Log index within the emitting transaction.
    #[serde(with = "alloy_serde::quantity")]
    pub log_index_in_tx: u64,
    /// Log index within the pending block.
    #[serde(with = "alloy_serde::quantity")]
    pub log_index_in_block: u64,
    /// Contract address that emitted the log.
    pub address: Address,
    /// Indexed log topics.
    pub topics: Vec<B256>,
    /// ABI-encoded log data.
    pub data: Bytes,
}

/// Transaction metadata referenced by a [`FastFlashblockLogsDelta`].
///
/// This intentionally mirrors the field layout of [`crate::FlashblockTxMeta`] for easy reuse, but
/// remains a separate hot-path local DTO paired with [`FastFlashblockLog`] rather than the
/// batch-oriented public subscription payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FastFlashblockTxMeta {
    /// Transaction hash.
    pub hash: B256,
    /// Transaction index within the pending block.
    #[serde(with = "alloy_serde::quantity")]
    pub index: u64,
    /// Optional transaction status using receipt-style quantity encoding.
    #[serde(default, skip_serializing_if = "Option::is_none", with = "alloy_serde::quantity::opt")]
    pub status: Option<u64>,
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256, Bytes};
    use alloy_rpc_types_engine::PayloadId;

    use super::*;

    fn test_snapshot_id() -> FlashblockSnapshotId {
        FlashblockSnapshotId::new(1, 2, 3, PayloadId::new([4; 8]), B256::with_last_byte(5))
    }

    fn test_log() -> FastFlashblockLog {
        FastFlashblockLog {
            tx_hash: B256::with_last_byte(7),
            tx_index: 8,
            log_index_in_tx: 0,
            log_index_in_block: 9,
            address: Address::with_last_byte(10),
            topics: vec![B256::with_last_byte(11)],
            data: Bytes::from_static(&[12, 13]),
        }
    }

    fn test_tx_meta() -> FastFlashblockTxMeta {
        FastFlashblockTxMeta { hash: B256::with_last_byte(14), index: 8, status: Some(1) }
    }

    fn test_delta() -> FastFlashblockLogsDelta {
        FastFlashblockLogsDelta::new(
            test_snapshot_id(),
            Some(6),
            vec![test_log()],
            vec![test_tx_meta()],
        )
    }

    #[test]
    fn snapshot_id_accessors_return_expected_values() {
        let snapshot_id = test_snapshot_id();

        assert_eq!(snapshot_id.nonce(), 1);
        assert_eq!(snapshot_id.block_number(), 2);
        assert_eq!(snapshot_id.flashblock_index(), 3);
        assert_eq!(snapshot_id.payload_id(), PayloadId::new([4; 8]));
        assert_eq!(snapshot_id.parent_hash(), B256::with_last_byte(5));
    }

    #[test]
    fn fast_flashblock_logs_delta_validate_identity_fields_rejects_mismatched_identity_fields() {
        let err = FastFlashblockLogsDelta {
            snapshot_id: test_snapshot_id(),
            block_number: 999,
            flashblock_index: 3,
            payload_id: PayloadId::new([4; 8]),
            parent_hash: B256::with_last_byte(5),
            block_timestamp: Some(6),
            logs: vec![test_log()],
            transactions: vec![test_tx_meta()],
        }
        .validate_identity_fields()
        .unwrap_err();

        assert_eq!(err, FastFlashblockLogsDeltaError::BlockNumberMismatch);
    }

    #[test]
    fn fast_flashblock_logs_delta_serializes_and_round_trips_snapshot_id_without_batch_hash() {
        let delta = test_delta();

        let value = serde_json::to_value(&delta).unwrap();
        let round_trip: FastFlashblockLogsDelta = serde_json::from_value(value.clone()).unwrap();

        assert!(value.get("snapshotId").is_some());
        assert!(value.get("batchHash").is_none());
        assert_eq!(value["snapshotId"]["nonce"], "0x1");
        assert_eq!(value["blockNumber"], "0x2");
        assert_eq!(value["flashblockIndex"], "0x3");
        assert_eq!(value["blockTimestamp"], "0x6");
        assert_eq!(value["logs"][0]["txIndex"], "0x8");
        assert_eq!(value["logs"][0]["logIndexInTx"], "0x0");
        assert_eq!(value["logs"][0]["logIndexInBlock"], "0x9");
        assert_eq!(value["transactions"][0]["status"], "0x1");
        assert_eq!(round_trip, delta);
    }

    #[test]
    fn fast_flashblock_logs_delta_serialize_rejects_mismatched_identity_fields() {
        let err = serde_json::to_value(&FastFlashblockLogsDelta {
            snapshot_id: test_snapshot_id(),
            block_number: 999,
            flashblock_index: 3,
            payload_id: PayloadId::new([4; 8]),
            parent_hash: B256::with_last_byte(5),
            block_timestamp: Some(6),
            logs: vec![test_log()],
            transactions: vec![test_tx_meta()],
        })
        .unwrap_err();

        assert!(err.to_string().contains("snapshot_id.block_number"));
    }

    #[test]
    fn fast_flashblock_logs_delta_deserialize_rejects_mismatched_identity_fields() {
        let mut value = serde_json::to_value(test_delta()).unwrap();
        value["blockNumber"] = serde_json::Value::String("0x3e7".to_string());

        let err = serde_json::from_value::<FastFlashblockLogsDelta>(value).unwrap_err();

        assert!(err.to_string().contains("snapshot_id.block_number"));
    }
}
