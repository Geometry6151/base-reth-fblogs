//! Subscription types for the `eth_` `PubSub` RPC extension

use alloy_consensus::Eip658Value;
use alloy_primitives::{Address, B256, Bloom, Bytes, keccak256};
use alloy_rpc_types_engine::PayloadId;
use alloy_rpc_types_eth::{Log, pubsub::SubscriptionKind};
use base_common_rpc_types::Transaction;
use derive_more::From;
use jsonrpsee_types::{ErrorObjectOwned, error::INVALID_PARAMS_CODE};
use serde::{Deserialize, Serialize};

use crate::FastFlashblockLogsDelta;

/// A full transaction object with its associated logs and receipt-equivalent fields.
///
/// This is returned by `newFlashblockTransactions` subscription when `full = true`
/// or when a log filter is provided, giving both the transaction details, logs emitted
/// by its execution, and receipt-derived fields already available from flashblock execution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionWithLogs {
    /// The full transaction object.
    #[serde(flatten)]
    pub transaction: Transaction,
    /// Logs emitted by this transaction.
    pub logs: Vec<Log>,
    /// Gas consumed by this transaction's execution.
    #[serde(with = "alloy_serde::quantity")]
    pub gas_used: u64,
    /// Status of the transaction, serialized the same way as `eth_getTransactionReceipt`.
    #[serde(flatten)]
    pub status: Eip658Value,
    /// Cumulative gas used in the block up to and including this transaction.
    #[serde(with = "alloy_serde::quantity")]
    pub cumulative_gas_used: u64,
    /// Contract address created, if this was a contract creation transaction.
    pub contract_address: Option<Address>,
    /// Bloom filter for all logs emitted by this transaction.
    pub logs_bloom: Bloom,
}

/// Batch-oriented flashblock logs payload for `newFlashblockLogsBatch` notifications.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlashblockLogsBatch {
    /// Block number for the pending block being assembled.
    #[serde(with = "alloy_serde::quantity")]
    pub block_number: u64,
    /// Index of this flashblock within the pending block.
    #[serde(with = "alloy_serde::quantity")]
    pub flashblock_index: u64,
    /// Engine payload identifier for the pending block.
    pub payload_id: PayloadId,
    /// Parent hash for the pending block.
    pub parent_hash: B256,
    /// Deterministic hash for this logs batch payload.
    pub batch_hash: B256,
    /// Optional timestamp for the pending block.
    #[serde(default, skip_serializing_if = "Option::is_none", with = "alloy_serde::quantity::opt")]
    pub block_timestamp: Option<u64>,
    /// Logs emitted in this flashblock update.
    pub logs: Vec<FlashblockLog>,
    /// Transaction metadata referenced by the logs in this batch.
    pub transactions: Vec<FlashblockTxMeta>,
}

/// Log entry included in a [`FlashblockLogsBatch`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlashblockLog {
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
    /// Whether the log was removed from pending state.
    pub removed: bool,
}

/// Transaction metadata referenced by a [`FlashblockLogsBatch`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlashblockTxMeta {
    /// Transaction hash.
    pub hash: B256,
    /// Transaction index within the pending block.
    #[serde(with = "alloy_serde::quantity")]
    pub index: u64,
    /// Optional transaction status using receipt-style quantity encoding.
    #[serde(default, skip_serializing_if = "Option::is_none", with = "alloy_serde::quantity::opt")]
    pub status: Option<u64>,
}

/// Returns an explicit compatibility error for pending-style RPC surfaces that are not available
/// in flashblocks hot-only mode.
pub(super) fn unsupported_in_hot_only(
    method: &'static str,
    replacement: Option<&'static str>,
) -> ErrorObjectOwned {
    let message = match replacement {
        Some(replacement) => {
            format!("{method} is unsupported in flashblocks hot-only mode; use {replacement}")
        }
        None => format!("{method} is unsupported in flashblocks hot-only mode"),
    };

    ErrorObjectOwned::owned(INVALID_PARAMS_CODE, message, None::<()>)
}

impl FlashblockLogsBatch {
    /// Returns the deterministic batch hash for this payload.
    pub fn compute_batch_hash(&self) -> B256 {
        let mut bytes = Vec::new();
        push_u64(&mut bytes, self.block_number);
        push_u64(&mut bytes, self.flashblock_index);
        bytes.extend_from_slice(self.payload_id.to_string().as_bytes());
        bytes.extend_from_slice(self.parent_hash.as_slice());

        for tx in &self.transactions {
            bytes.extend_from_slice(tx.hash.as_slice());
            push_u64(&mut bytes, tx.index);
            push_u64(&mut bytes, tx.status.unwrap_or(u64::MAX));
        }

        for log in &self.logs {
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

    /// Recomputes and stores the deterministic batch hash for this payload.
    pub fn refresh_batch_hash(&mut self) {
        self.batch_hash = self.compute_batch_hash();
    }

    /// Builds a batch notification payload from a fast delta without consulting pending state.
    pub fn from_fast_delta(delta: &FastFlashblockLogsDelta) -> Self {
        let logs = delta
            .logs
            .iter()
            .map(|log| FlashblockLog {
                tx_hash: log.tx_hash,
                tx_index: log.tx_index,
                log_index_in_tx: log.log_index_in_tx,
                log_index_in_block: log.log_index_in_block,
                address: log.address,
                topics: log.topics.clone(),
                data: log.data.clone(),
                removed: false,
            })
            .collect();
        let transactions = delta
            .transactions
            .iter()
            .map(|tx| FlashblockTxMeta { hash: tx.hash, index: tx.index, status: tx.status })
            .collect();

        let mut batch = Self {
            block_number: delta.block_number,
            flashblock_index: delta.flashblock_index,
            payload_id: delta.payload_id,
            parent_hash: delta.parent_hash,
            batch_hash: B256::ZERO,
            block_timestamp: delta.block_timestamp,
            logs,
            transactions,
        };
        batch.refresh_batch_hash();
        batch
    }
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

/// Extended subscription kind that includes both standard Ethereum subscription types
/// and flashblocks-specific types.
///
/// This enum encapsulates the standard [`SubscriptionKind`] from alloy and adds flashblocks
/// support, allowing `eth_subscribe` to handle both standard subscriptions (newHeads, logs, etc.)
/// and custom flashblocks subscriptions.
///
/// By encapsulating [`SubscriptionKind`] rather than redefining its variants, we automatically
/// inherit support for any new variants added upstream, or get a compile error if the signature
/// changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, From)]
#[serde(untagged)]
pub enum ExtendedSubscriptionKind {
    /// Standard Ethereum subscription types (newHeads, logs, newPendingTransactions, syncing).
    ///
    /// These are proxied to reth's underlying `EthPubSub` implementation.
    #[from]
    Standard(SubscriptionKind),
    /// Base-specific subscription types for flashblocks.
    #[from]
    Base(BaseSubscriptionKind),
}

/// Base-specific subscription types for flashblocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BaseSubscriptionKind {
    /// New flashblocks subscription.
    ///
    /// Fires a notification each time a new flashblock is processed, providing the current
    /// pending block state. Each flashblock represents an incremental update to the pending
    /// block, so multiple notifications may be emitted for the same block height as new
    /// flashblocks arrive.
    NewFlashblocks,
    /// Pending logs subscription.
    ///
    /// Returns logs from flashblocks pending state that match the given filter criteria.
    /// Unlike standard `logs` subscription which only includes logs from confirmed blocks,
    /// this includes logs from the current pending flashblock state.
    PendingLogs,
    /// New flashblock transactions subscription.
    ///
    /// Returns transactions from flashblocks as they are sequenced, providing higher inclusion
    /// confidence than standard `newPendingTransactions` which returns mempool transactions.
    /// Flashblock transactions have been included by the sequencer and are effectively preconfirmed.
    ///
    /// Accepts an optional parameter:
    /// - `true`: Returns full transaction objects with their associated logs (as
    ///   [`TransactionWithLogs`])
    /// - `false` (default): Returns only transaction hashes
    /// - A log filter object (with `address` and/or `topics`): Returns full transaction objects
    ///   where at least one log matches the filter. All logs are included in the response, not
    ///   just the matching ones.
    NewFlashblockTransactions,
    /// New flashblock logs batch subscription.
    ///
    /// Returns batch-oriented log updates for each flashblock applied to the pending block,
    /// including per-log indices and referenced transaction metadata.
    NewFlashblockLogsBatch,
    /// New fast flashblock logs subscription.
    ///
    /// Returns the local fast-path logs delta emitted by the in-process fast-update broadcaster.
    /// This payload includes a `snapshotId` cursor but intentionally does not include a
    /// `batchHash`.
    NewFastFlashblockLogs,
}

impl ExtendedSubscriptionKind {
    /// Returns the standard subscription kind if this is a standard subscription type.
    pub const fn as_standard(&self) -> Option<SubscriptionKind> {
        match self {
            Self::Standard(kind) => Some(*kind),
            Self::Base(_) => None,
        }
    }

    /// Returns true if this is a flashblocks-specific subscription.
    pub const fn is_flashblocks(&self) -> bool {
        matches!(self, Self::Base(_))
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{Signed, transaction::Recovered};
    use alloy_primitives::{
        Address, B256, Bytes, Log as PrimitiveLog, LogData, Signature, TxKind, U256,
    };
    use alloy_rpc_types_engine::PayloadId;
    use alloy_rpc_types_eth::Log;
    use base_common_consensus::BaseTxEnvelope;
    use base_common_rpc_types::Transaction;

    use super::*;

    fn test_transaction_with_logs() -> TransactionWithLogs {
        let legacy = alloy_consensus::TxLegacy {
            chain_id: Some(1),
            nonce: 7,
            gas_price: 1_000_000_000,
            gas_limit: 21_000,
            to: TxKind::Call(Address::with_last_byte(0xBB)),
            value: U256::from(1_000_000u64),
            input: Bytes::new(),
        };
        let hash = B256::with_last_byte(0xAA);
        let envelope = BaseTxEnvelope::Legacy(Signed::new_unchecked(
            legacy,
            Signature::test_signature(),
            hash,
        ));
        let recovered = Recovered::new_unchecked(envelope, Address::with_last_byte(0xCC));
        let tx = Transaction {
            inner: alloy_rpc_types_eth::Transaction {
                inner: recovered,
                block_hash: Some(B256::ZERO),
                block_number: Some(42),
                transaction_index: Some(3),
                effective_gas_price: Some(1_000_000_000),
            },
            deposit_nonce: None,
            deposit_receipt_version: None,
        };

        let log = Log {
            inner: PrimitiveLog {
                address: Address::with_last_byte(0xDD),
                data: LogData::new_unchecked(
                    vec![B256::with_last_byte(0xEE)],
                    Bytes::from_static(&[0x01, 0x02]),
                ),
            },
            block_hash: Some(B256::ZERO),
            block_number: Some(42),
            block_timestamp: None,
            transaction_hash: Some(hash),
            transaction_index: Some(3),
            log_index: Some(0),
            removed: false,
        };

        TransactionWithLogs {
            transaction: tx,
            logs: vec![log],
            gas_used: 21_000,
            status: Eip658Value::Eip658(true),
            cumulative_gas_used: 42_000,
            contract_address: Some(Address::with_last_byte(0xEF)),
            logs_bloom: [0x11; 256].into(),
        }
    }

    #[test]
    fn transaction_with_logs_json_format() {
        let twl = test_transaction_with_logs();
        let json = serde_json::to_value(&twl).expect("serialization should succeed");
        let obj = json.as_object().expect("should be a JSON object");

        assert!(obj.contains_key("logs"), "missing 'logs' field");
        assert!(obj.contains_key("gasUsed"), "missing 'gasUsed' field");
        assert!(obj.contains_key("status"), "missing 'status' field");
        assert!(obj.contains_key("cumulativeGasUsed"), "missing 'cumulativeGasUsed' field");
        assert!(obj.contains_key("contractAddress"), "missing 'contractAddress' field");
        assert!(obj.contains_key("logsBloom"), "missing 'logsBloom' field");
        assert!(obj.contains_key("nonce"), "missing flattened tx 'nonce' field");
        assert!(obj.contains_key("gasPrice"), "missing flattened tx 'gasPrice' field");
        assert!(obj.contains_key("hash"), "missing flattened tx 'hash' field");
        assert!(obj.contains_key("from"), "missing flattened tx 'from' field");
        assert!(obj.contains_key("to"), "missing flattened tx 'to' field");
        assert!(obj.contains_key("value"), "missing flattened tx 'value' field");
        assert!(obj.contains_key("blockNumber"), "missing flattened tx 'blockNumber' field");

        assert_eq!(obj["gasUsed"], "0x5208", "gasUsed should use receipt quantity encoding");
        assert_eq!(obj["status"], "0x1", "status should use receipt quantity encoding");
        assert_eq!(
            obj["cumulativeGasUsed"], "0xa410",
            "cumulativeGasUsed should use receipt quantity encoding"
        );
        assert_eq!(
            obj["contractAddress"],
            format!("{:#x}", Address::with_last_byte(0xEF)),
            "contractAddress should serialize as an address"
        );
        assert_eq!(
            obj["logsBloom"],
            format!("0x{}", "11".repeat(256)),
            "logsBloom should serialize as a bloom hex string"
        );

        let logs = obj["logs"].as_array().expect("logs should be an array");
        assert_eq!(logs.len(), 1);
        let log = logs[0].as_object().expect("log should be a JSON object");
        assert!(log.contains_key("address"), "log missing 'address' field");
        assert!(log.contains_key("topics"), "log missing 'topics' field");
        assert!(log.contains_key("data"), "log missing 'data' field");
        assert!(log.contains_key("transactionHash"), "log missing 'transactionHash' field");
    }

    #[test]
    fn transaction_with_logs_json_roundtrip() {
        let original = test_transaction_with_logs();
        let json_str = serde_json::to_string(&original).expect("serialization should succeed");
        let deserialized: TransactionWithLogs =
            serde_json::from_str(&json_str).expect("deserialization should succeed");

        assert_eq!(original, deserialized);
    }

    #[test]
    fn transaction_with_logs_json_string_contains_expected_fields() {
        let twl = test_transaction_with_logs();
        let json_str = serde_json::to_string(&twl).expect("serialization should succeed");

        assert!(
            json_str.contains("\"gasUsed\":\"0x5208\""),
            "JSON must contain gasUsed key with quantity encoding"
        );
        assert!(json_str.contains("\"status\":\"0x1\""), "JSON must contain status key");
        assert!(
            json_str.contains("\"cumulativeGasUsed\":\"0xa410\""),
            "JSON must contain cumulativeGasUsed key"
        );
        assert!(json_str.contains("\"contractAddress\""), "JSON must contain contractAddress key");
        assert!(json_str.contains("\"logsBloom\""), "JSON must contain logsBloom key");
        assert!(json_str.contains("\"logs\""), "JSON must contain logs key");
        assert!(json_str.contains("\"gasPrice\""), "JSON must contain gasPrice key");
        assert!(json_str.contains("\"nonce\""), "JSON must contain nonce key");
        assert!(json_str.contains("\"hash\""), "JSON must contain hash key");
        assert!(json_str.contains("\"from\""), "JSON must contain from key");
        assert!(json_str.contains("\"to\""), "JSON must contain to key");
        assert!(json_str.contains("\"blockNumber\""), "JSON must contain blockNumber key");
        assert!(json_str.contains("\"topics\""), "JSON must contain topics key in logs");
        assert!(json_str.contains("\"address\""), "JSON must contain address key in logs");
        assert!(
            json_str.contains("\"transactionHash\""),
            "JSON must contain transactionHash key in logs"
        );
    }

    #[test]
    fn transaction_with_logs_contract_address_none_serialization() {
        let mut twl = test_transaction_with_logs();
        twl.contract_address = None;
        let json = serde_json::to_value(&twl).expect("serialization should succeed");
        let obj = json.as_object().expect("should be a JSON object");

        assert!(
            obj.contains_key("contractAddress"),
            "contractAddress key should be present even when None"
        );
        assert!(obj["contractAddress"].is_null(), "contractAddress should be null when None");
        assert_eq!(obj["gasUsed"], "0x5208", "gasUsed should remain a required quantity field");
        assert_eq!(obj["status"], "0x1", "status should remain a required receipt field");
        assert_eq!(
            obj["cumulativeGasUsed"], "0xa410",
            "cumulativeGasUsed should remain a required quantity field"
        );
        assert_eq!(
            obj["logsBloom"],
            format!("0x{}", "11".repeat(256)),
            "logsBloom should remain a required bloom field"
        );
    }

    #[test]
    fn base_subscription_kind_decodes_new_flashblock_logs_batch() {
        let kind: BaseSubscriptionKind =
            serde_json::from_str(r#""newFlashblockLogsBatch""#).unwrap();
        assert_eq!(kind, BaseSubscriptionKind::NewFlashblockLogsBatch);

        let encoded = serde_json::to_string(&kind).unwrap();
        assert_eq!(encoded, r#""newFlashblockLogsBatch""#);
    }

    #[test]
    fn base_subscription_kind_decodes_new_fast_flashblock_logs() {
        let kind: BaseSubscriptionKind =
            serde_json::from_str(r#""newFastFlashblockLogs""#).unwrap();
        assert_eq!(kind, BaseSubscriptionKind::NewFastFlashblockLogs);

        let encoded = serde_json::to_string(&kind).unwrap();
        assert_eq!(encoded, r#""newFastFlashblockLogs""#);
    }

    #[test]
    fn flashblock_logs_batch_serializes_contract_shape() {
        let batch = FlashblockLogsBatch {
            block_number: 1,
            flashblock_index: 2,
            payload_id: PayloadId::new([3; 8]),
            parent_hash: B256::with_last_byte(4),
            batch_hash: B256::with_last_byte(5),
            block_timestamp: Some(6),
            logs: vec![FlashblockLog {
                tx_hash: B256::with_last_byte(7),
                tx_index: 8,
                log_index_in_tx: 0,
                log_index_in_block: 9,
                address: Address::with_last_byte(10),
                topics: vec![B256::with_last_byte(11)],
                data: Bytes::from_static(&[12, 13]),
                removed: false,
            }],
            transactions: vec![FlashblockTxMeta {
                hash: B256::with_last_byte(14),
                index: 8,
                status: Some(1),
            }],
        };

        let value = serde_json::to_value(&batch).unwrap();
        assert_eq!(value["blockNumber"], "0x1");
        assert_eq!(value["flashblockIndex"], "0x2");
        assert_eq!(value["blockTimestamp"], "0x6");
        assert_eq!(value["logs"][0]["txIndex"], "0x8");
        assert_eq!(value["logs"][0]["logIndexInTx"], "0x0");
        assert_eq!(value["logs"][0]["logIndexInBlock"], "0x9");
        assert_eq!(value["transactions"][0]["status"], "0x1");
    }

    #[test]
    fn flashblock_logs_batch_from_fast_delta_sets_removed_false_and_hash() {
        let delta = crate::FastFlashblockLogsDelta::new(
            crate::FlashblockSnapshotId::new(
                1,
                2,
                3,
                PayloadId::new([4; 8]),
                B256::with_last_byte(5),
            ),
            Some(6),
            vec![crate::FastFlashblockLog {
                tx_hash: B256::with_last_byte(7),
                tx_index: 8,
                log_index_in_tx: 0,
                log_index_in_block: 9,
                address: Address::with_last_byte(10),
                topics: vec![B256::with_last_byte(11)],
                data: Bytes::from_static(&[12, 13]),
            }],
            vec![crate::FastFlashblockTxMeta {
                hash: B256::with_last_byte(14),
                index: 8,
                status: Some(1),
            }],
        );

        let batch = FlashblockLogsBatch::from_fast_delta(&delta);

        assert_eq!(batch.block_number, delta.block_number);
        assert_eq!(batch.flashblock_index, delta.flashblock_index);
        assert_eq!(batch.payload_id, delta.payload_id);
        assert_eq!(batch.parent_hash, delta.parent_hash);
        assert_eq!(batch.block_timestamp, delta.block_timestamp);
        assert_eq!(batch.logs.len(), 1);
        assert!(!batch.logs[0].removed);
        assert_eq!(batch.transactions.len(), 1);
        assert_eq!(batch.batch_hash, batch.compute_batch_hash());
    }
}
